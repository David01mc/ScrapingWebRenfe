"""
Captura unificada Renfe en tiempo real — Cercanías Asturias, Cercanías Cádiz y Largo Recorrido
==============================================================================================
Optimizaciones activas:
  - Un único proceso y una única conexión a BD cada flush (3× menos conexiones)
  - Flush cada 20 minutos (40 ciclos × 30s) — BD auto-pausable entre flushes
  - Pausa nocturna 23:00–06:00 — sin capturas ni conexiones de madrugada
  - Itinerarios de largo recorrido guardados una sola vez por tren por día

Uso:
  python renfe_capture.py                    # Captura única
  python renfe_capture.py --loop 30          # Loop con flush cada 20 min
  python renfe_capture.py --summary          # Resumen de todas las tablas
  python renfe_capture.py --init-stations    # Cargar estaciones (solo la primera vez)
  python renfe_capture.py --loop 30 --flush-every 20  # Flush cada 10 min
"""

import math
import re
import requests
import argparse
import time
import logging
import sys
from datetime import datetime, timedelta, timezone

import pyodbc
from azure_db import get_conn, run_ddl

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_FILE = "/opt/renfe/renfe-capture.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ╔══════════════════════════════════════════════════════════════╗
# ║  CONFIGURACIÓN                                               ║
# ╚══════════════════════════════════════════════════════════════╝

FLUSH_CYCLES = 40   # 40 × 30s = 20 minutos
NIGHT_START  = 23   # hora de inicio pausa nocturna
NIGHT_END    = 6    # hora de fin pausa nocturna

ASTURIAS_BBOX = {"lat_min": 43.0, "lat_max": 43.7, "lon_min": -7.0, "lon_max": -4.5}
CADIZ_BBOX    = {"lat_min": 36.3, "lat_max": 37.5, "lon_min": -6.5, "lon_max": -5.7}

ENDPOINTS_CERC = {
    "vehicle_positions": "https://gtfsrt.renfe.com/vehicle_positions.json",
    "trip_updates":      "https://gtfsrt.renfe.com/trip_updates.json",
    "service_alerts":    "https://gtfsrt.renfe.com/alerts.json",
}

BASE_LARGO = "https://tiempo-real.largorecorrido.renfe.com"
ENDPOINTS_LARGO = {
    "flota":       f"{BASE_LARGO}/renfe-visor/flotaLD.json",
    "itinerarios": f"{BASE_LARGO}/renfe-visor/trenesConEstacionesLD.json",
    "estaciones":  f"{BASE_LARGO}/data/estaciones.geojson",
}

CADIZ_MADRID_KEYWORDS = ["ádiz", "adiz"]

ASTURIAS_ALERT_KEYWORDS = [
    "asturias", "oviedo", "gijón", "gijon", "avilés", "aviles",
    "mieres", "langreo", "laviana", "el entrego", "pola de lena",
    "la corredoria", "llamaquique", "feve",
]
CADIZ_ALERT_KEYWORDS = [
    "cádiz", "cadiz", "san fernando", "el puerto", "jerez",
    "sevilla", "chiclana", "puerto real", "barbate",
]

TIPOS_TREN = {
    1: "AVE", 2: "Larga Distancia", 3: "Avant", 4: "Alvia",
    5: "Alaris", 6: "Altaria", 7: "Arco", 8: "Euromed",
    9: "Talgo", 10: "Intercity", 11: "Media Distancia",
    12: "Regional", 13: "Regional Exprés",
}

REQUEST_TIMEOUT = 15

# ╔══════════════════════════════════════════════════════════════╗
# ║  BASE DE DATOS — INIT                                        ║
# ╚══════════════════════════════════════════════════════════════╝

def init_db(conn: pyodbc.Connection):
    """Crea todas las tablas e índices si no existen."""
    run_ddl(conn, [
        # ── Asturias ─────────────────────────────────────────────
        """
        IF OBJECT_ID('asturias_vehicle_snapshots','U') IS NULL
        CREATE TABLE asturias_vehicle_snapshots (
            id INT IDENTITY(1,1) PRIMARY KEY, captured_at NVARCHAR(50) NOT NULL,
            feed_timestamp BIGINT, trip_id NVARCHAR(100), vehicle_id NVARCHAR(50),
            vehicle_label NVARCHAR(200), line NVARCHAR(20), latitude FLOAT,
            longitude FLOAT, bearing FLOAT, speed FLOAT,
            current_status NVARCHAR(50), stop_id NVARCHAR(50), event_timestamp BIGINT
        )""",
        """
        IF OBJECT_ID('asturias_trip_updates','U') IS NULL
        CREATE TABLE asturias_trip_updates (
            id INT IDENTITY(1,1) PRIMARY KEY, captured_at NVARCHAR(50) NOT NULL,
            feed_timestamp BIGINT, trip_id NVARCHAR(100), line NVARCHAR(20),
            schedule_relationship NVARCHAR(50), stop_sequence INT,
            stop_id NVARCHAR(50), arrival_delay INT, departure_delay INT
        )""",
        """
        IF OBJECT_ID('asturias_service_alerts','U') IS NULL
        CREATE TABLE asturias_service_alerts (
            id INT IDENTITY(1,1) PRIMARY KEY, captured_at NVARCHAR(50) NOT NULL,
            feed_timestamp BIGINT, alert_id NVARCHAR(100), cause NVARCHAR(100),
            effect NVARCHAR(100), header_text NVARCHAR(MAX), description NVARCHAR(MAX)
        )""",
        # ── Cádiz ────────────────────────────────────────────────
        """
        IF OBJECT_ID('cadiz_vehicle_snapshots','U') IS NULL
        CREATE TABLE cadiz_vehicle_snapshots (
            id INT IDENTITY(1,1) PRIMARY KEY, captured_at NVARCHAR(50) NOT NULL,
            feed_timestamp BIGINT, trip_id NVARCHAR(100), vehicle_id NVARCHAR(50),
            vehicle_label NVARCHAR(200), line NVARCHAR(20), latitude FLOAT,
            longitude FLOAT, bearing FLOAT, speed FLOAT,
            current_status NVARCHAR(50), stop_id NVARCHAR(50), event_timestamp BIGINT
        )""",
        """
        IF OBJECT_ID('cadiz_trip_updates','U') IS NULL
        CREATE TABLE cadiz_trip_updates (
            id INT IDENTITY(1,1) PRIMARY KEY, captured_at NVARCHAR(50) NOT NULL,
            feed_timestamp BIGINT, trip_id NVARCHAR(100), line NVARCHAR(20),
            schedule_relationship NVARCHAR(50), stop_sequence INT,
            stop_id NVARCHAR(50), arrival_delay INT, departure_delay INT
        )""",
        """
        IF OBJECT_ID('cadiz_service_alerts','U') IS NULL
        CREATE TABLE cadiz_service_alerts (
            id INT IDENTITY(1,1) PRIMARY KEY, captured_at NVARCHAR(50) NOT NULL,
            feed_timestamp BIGINT, alert_id NVARCHAR(100), cause NVARCHAR(100),
            effect NVARCHAR(100), header_text NVARCHAR(MAX), description NVARCHAR(MAX)
        )""",
        # ── Largo Recorrido ──────────────────────────────────────
        """
        IF OBJECT_ID('train_snapshots','U') IS NULL
        CREATE TABLE train_snapshots (
            id INT IDENTITY(1,1) PRIMARY KEY, captured_at NVARCHAR(50) NOT NULL,
            feed_timestamp NVARCHAR(50), cod_comercial NVARCHAR(20), cod_product INT,
            tipo_tren NVARCHAR(50), des_corridor NVARCHAR(200),
            cod_origen NVARCHAR(20), cod_destino NVARCHAR(20),
            latitude FLOAT, longitude FLOAT, speed FLOAT, bearing FLOAT,
            ult_retraso INT, cod_est_ant NVARCHAR(20), cod_est_sig NVARCHAR(20),
            hora_llegada_sig_est NVARCHAR(20), plataforma NVARCHAR(20),
            material NVARCHAR(50), accesible INT, gps_timestamp BIGINT
        )""",
        """
        IF OBJECT_ID('train_itineraries','U') IS NULL
        CREATE TABLE train_itineraries (
            id INT IDENTITY(1,1) PRIMARY KEY, captured_at NVARCHAR(50) NOT NULL,
            cod_comercial NVARCHAR(20), stop_order INT, station_code NVARCHAR(20),
            hora_prog NVARCHAR(10), latitude FLOAT, longitude FLOAT
        )""",
        """
        IF OBJECT_ID('stations','U') IS NULL
        CREATE TABLE stations (
            codigo INT PRIMARY KEY, nombre NVARCHAR(200),
            latitude FLOAT, longitude FLOAT, accesible INT,
            direccion NVARCHAR(300), localidad NVARCHAR(100),
            provincia NVARCHAR(100), es_cercanias INT, nivel INT
        )""",
        # ── Índices ──────────────────────────────────────────────
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_ast_vs_captured' AND object_id=OBJECT_ID('asturias_vehicle_snapshots')) CREATE INDEX idx_ast_vs_captured ON asturias_vehicle_snapshots(captured_at)",
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_ast_vs_vehicle_ts' AND object_id=OBJECT_ID('asturias_vehicle_snapshots')) CREATE INDEX idx_ast_vs_vehicle_ts ON asturias_vehicle_snapshots(vehicle_id, event_timestamp DESC)",
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_ast_tu_trip' AND object_id=OBJECT_ID('asturias_trip_updates')) CREATE INDEX idx_ast_tu_trip ON asturias_trip_updates(trip_id)",
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_cdz_vs_captured' AND object_id=OBJECT_ID('cadiz_vehicle_snapshots')) CREATE INDEX idx_cdz_vs_captured ON cadiz_vehicle_snapshots(captured_at)",
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_cdz_vs_vehicle_ts' AND object_id=OBJECT_ID('cadiz_vehicle_snapshots')) CREATE INDEX idx_cdz_vs_vehicle_ts ON cadiz_vehicle_snapshots(vehicle_id, event_timestamp DESC)",
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_ts_captured' AND object_id=OBJECT_ID('train_snapshots')) CREATE INDEX idx_ts_captured ON train_snapshots(captured_at)",
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_ts_tren' AND object_id=OBJECT_ID('train_snapshots')) CREATE INDEX idx_ts_tren ON train_snapshots(cod_comercial)",
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_ts_gps' AND object_id=OBJECT_ID('train_snapshots')) CREATE INDEX idx_ts_gps ON train_snapshots(cod_comercial, gps_timestamp DESC)",
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='idx_ti_tren' AND object_id=OBJECT_ID('train_itineraries')) CREATE INDEX idx_ti_tren ON train_itineraries(cod_comercial)",
    ])
    print("Tablas e índices verificados (Azure SQL — Unificado).")


# ╔══════════════════════════════════════════════════════════════╗
# ║  PAUSA NOCTURNA                                              ║
# ╚══════════════════════════════════════════════════════════════╝

def wait_if_night(batch: dict) -> int:
    """
    Si es horario nocturno (23:00–06:00 UTC), hace flush de los datos
    pendientes y duerme hasta las 06:00. Devuelve el cycle reseteado (0).
    """
    now = datetime.now(timezone.utc)
    if now.hour >= NIGHT_START or now.hour < NIGHT_END:
        # Volcar datos pendientes antes de dormir
        if any(batch[k] for k in batch):
            print(f"[{now.strftime('%Y-%m-%d %H:%M')} UTC] Flush pre-pausa nocturna...")
            conn = get_conn()
            flush_batch(conn, batch)
            conn.close()

        if now.hour >= NIGHT_START:
            dawn = (now + timedelta(days=1)).replace(
                hour=NIGHT_END, minute=0, second=0, microsecond=0)
        else:
            dawn = now.replace(hour=NIGHT_END, minute=0, second=0, microsecond=0)
        secs = (dawn - now).total_seconds()
        print(
            f"[{now.strftime('%Y-%m-%d %H:%M')} UTC] "
            f"Pausa nocturna — reanudando a las {NIGHT_END:02d}:00 UTC "
            f"({secs / 3600:.1f}h)"
        )
        time.sleep(secs)
        return 0  # resetear cycle tras despertar
    return -1     # indicador: no era de noche, cycle sin cambios


# ╔══════════════════════════════════════════════════════════════╗
# ║  VELOCIDAD Y BEARING                                         ║
# ╚══════════════════════════════════════════════════════════════╝

MIN_SPEED_KMH        = 0.5
MAX_SPEED_CERC_KMH   = 200.0
MAX_SPEED_LARGO_KMH  = 350.0


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def calculate_bearing(lat1, lon1, lat2, lon2):
    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    dlon_r = math.radians(lon2 - lon1)
    x = math.sin(dlon_r) * math.cos(lat2_r)
    y = (math.cos(lat1_r) * math.sin(lat2_r) -
         math.sin(lat1_r) * math.cos(lat2_r) * math.cos(dlon_r))
    return math.degrees(math.atan2(x, y)) % 360


def compute_speed_bearing(lat1, lon1, ts1, lat2, lon2, ts2, max_speed=MAX_SPEED_CERC_KMH):
    if None in (lat1, lon1, ts1, lat2, lon2, ts2):
        return None, None
    dt = int(ts2) - int(ts1)
    if dt <= 0:
        return None, None
    speed = haversine_km(lat1, lon1, lat2, lon2) / (dt / 3600)
    if speed < MIN_SPEED_KMH or speed > max_speed:
        return None, None
    return round(speed, 2), round(calculate_bearing(lat1, lon1, lat2, lon2), 1)


# ╔══════════════════════════════════════════════════════════════╗
# ║  CACHÉS EN MEMORIA                                           ║
# ╚══════════════════════════════════════════════════════════════╝

# Cercanías Asturias: vehicle_id -> (lat, lon, event_ts, captured_at_iso)
_ast_pos:   dict = {}
# (vehicle_id, lat4, lon4) -> primera_captured_at_iso en esa posición
_ast_first: dict = {}
_ast_trips: set  = set()

# Cercanías Cádiz
_cdz_pos:   dict = {}
_cdz_first: dict = {}
_cdz_trips: set  = set()

# Largo Recorrido: cod_comercial -> (lat, lon, gps_ts, captured_at_iso)
_lr_pos: dict = {}

# Itinerarios ya guardados hoy: set de (cod_comercial, "YYYY-MM-DD")
_itin_today:     set = set()
_itin_today_date: str = ""


def _rollover_itineraries():
    """Limpia el set de itinerarios si cambió el día (UTC)."""
    global _itin_today, _itin_today_date
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if today != _itin_today_date:
        _itin_today.clear()
        _itin_today_date = today


# ╔══════════════════════════════════════════════════════════════╗
# ║  HELPERS                                                     ║
# ╚══════════════════════════════════════════════════════════════╝

def fetch_json(url):
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def extract_line(label):
    if not label:
        return None
    return label.split("-")[0] if "-" in label else None


def _cerc_speed_bearing(pos_cache, first_cache, vehicle_id, lat, lon, now_iso):
    """Calcula speed/bearing para cercanías usando caché en memoria."""
    prev = pos_cache.get(vehicle_id)
    if not prev:
        return None, None
    prev_lat, prev_lon, _, prev_cap = prev
    if abs(lat - prev_lat) <= 0.0001 and abs(lon - prev_lon) <= 0.0001:
        return None, None

    first_iso = first_cache.get((vehicle_id, round(prev_lat, 4), round(prev_lon, 4)))
    try:
        ts1 = datetime.fromisoformat(first_iso or prev_cap).timestamp()
    except Exception:
        return None, None
    ts2 = datetime.fromisoformat(now_iso).timestamp()
    return compute_speed_bearing(prev_lat, prev_lon, ts1, lat, lon, ts2, MAX_SPEED_CERC_KMH)


def _update_cerc_cache(pos_cache, first_cache, vehicle_id, lat, lon, event_ts, now_iso):
    """Actualiza los cachés de posición de cercanías."""
    pos_key = (vehicle_id, round(lat, 4), round(lon, 4))
    if pos_key not in first_cache:
        first_cache[pos_key] = now_iso
    # Si el vehículo se movió, registrar primera vez en la nueva posición
    prev = pos_cache.get(vehicle_id)
    if prev:
        prev_lat, prev_lon = prev[0], prev[1]
        if abs(lat - prev_lat) > 0.0001 or abs(lon - prev_lon) > 0.0001:
            new_key = (vehicle_id, round(lat, 4), round(lon, 4))
            if new_key not in first_cache:
                first_cache[new_key] = now_iso
    pos_cache[vehicle_id] = (lat, lon, event_ts, now_iso)


# ╔══════════════════════════════════════════════════════════════╗
# ║  PROCESAMIENTO — CERCANÍAS ASTURIAS                          ║
# ╚══════════════════════════════════════════════════════════════╝

def _process_cerc_positions(bbox, pos_cache, first_cache, known_trips) -> list:
    data = fetch_json(ENDPOINTS_CERC["vehicle_positions"])
    feed_ts = data.get("header", {}).get("timestamp")
    now = datetime.now(timezone.utc).isoformat()

    rows = []
    for entity in data.get("entity", []):
        v = entity.get("vehicle", {})
        pos = v.get("position", {})
        lat, lon = pos.get("latitude"), pos.get("longitude")
        if lat is None or lon is None:
            continue
        if not (bbox["lat_min"] <= lat <= bbox["lat_max"] and
                bbox["lon_min"] <= lon <= bbox["lon_max"]):
            continue

        veh = v.get("vehicle", {})
        vid = veh.get("id")
        event_ts = v.get("timestamp")
        speed, bearing = _cerc_speed_bearing(pos_cache, first_cache, vid, lat, lon, now)
        _update_cerc_cache(pos_cache, first_cache, vid, lat, lon, event_ts, now)

        label = veh.get("label", "")
        trip_id = v.get("trip", {}).get("tripId")
        if trip_id:
            known_trips.add(trip_id)
        rows.append((
            now, feed_ts, trip_id, vid, label, extract_line(label),
            lat, lon, bearing, speed,
            v.get("currentStatus"), v.get("stopId"), event_ts,
        ))
    return rows


def _process_cerc_trip_updates(known_trips) -> list:
    if not known_trips:
        return []
    data = fetch_json(ENDPOINTS_CERC["trip_updates"])
    feed_ts = data.get("header", {}).get("timestamp")
    now = datetime.now(timezone.utc).isoformat()

    rows = []
    for entity in data.get("entity", []):
        tu = entity.get("tripUpdate", {})
        trip = tu.get("trip", {})
        trip_id = trip.get("tripId")
        if trip_id not in known_trips:
            continue
        schedule_rel = trip.get("scheduleRelationship")
        stus = tu.get("stopTimeUpdate", [])
        if not stus:
            rows.append((now, feed_ts, trip_id, None, schedule_rel, None, None, None, None))
        else:
            for stu in stus:
                line = None
                if trip_id:
                    parts = trip_id.split("L")
                    if len(parts) == 2:
                        m = re.search(r'([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*)$', parts[1])
                        line = m.group(1) if m else None
                arr = stu.get("arrival", {})
                dep = stu.get("departure", {})
                rows.append((
                    now, feed_ts, trip_id, line, schedule_rel,
                    stu.get("stopSequence"), stu.get("stopId"),
                    arr.get("delay"), dep.get("delay"),
                ))
    return rows


def _process_cerc_alerts(keywords) -> list:
    data = fetch_json(ENDPOINTS_CERC["service_alerts"])
    feed_ts = data.get("header", {}).get("timestamp")
    now = datetime.now(timezone.utc).isoformat()

    rows = []
    for entity in data.get("entity", []):
        alert = entity.get("alert", {})
        h_tr = alert.get("headerText", {}).get("translation", [])
        d_tr = alert.get("descriptionText", {}).get("translation", [])
        header = h_tr[0].get("text", "") if h_tr else ""
        desc   = d_tr[0].get("text", "") if d_tr else ""
        if not any(kw in (header + " " + desc).lower() for kw in keywords):
            continue
        rows.append((now, feed_ts, entity.get("id"),
                     alert.get("cause"), alert.get("effect"), header, desc))
    return rows


# ╔══════════════════════════════════════════════════════════════╗
# ║  PROCESAMIENTO — LARGO RECORRIDO                             ║
# ╚══════════════════════════════════════════════════════════════╝

def _resolve_ts(prev_gps, prev_cap, curr_gps, curr_cap):
    try:
        if prev_gps is not None and curr_gps is not None and int(curr_gps) != int(prev_gps):
            return int(prev_gps), int(curr_gps)
        return (datetime.fromisoformat(prev_cap).timestamp(),
                datetime.fromisoformat(curr_cap).timestamp())
    except Exception:
        return None, None


def _process_largo_flota() -> list:
    data = fetch_json(ENDPOINTS_LARGO["flota"])
    feed_ts = data.get("fechaActualizacion")
    now = datetime.now(timezone.utc).isoformat()

    rows = []
    for tren in data.get("trenes", []):

        cod = tren.get("codComercial")
        lat, lon = tren.get("latitud"), tren.get("longitud")
        gps_ts = tren.get("time")
        cod_product = tren.get("codProduct")

        speed, bearing = None, None
        prev = _lr_pos.get(cod)
        if prev:
            ts1, ts2 = _resolve_ts(prev[2], prev[3], gps_ts, now)
            speed, bearing = compute_speed_bearing(
                prev[0], prev[1], ts1, lat, lon, ts2, MAX_SPEED_LARGO_KMH)
        if cod:
            _lr_pos[cod] = (lat, lon, gps_ts, now)

        try:
            retraso = int(tren.get("ultRetraso", 0))
        except (ValueError, TypeError):
            retraso = None

        rows.append((
            now, feed_ts, cod, cod_product,
            TIPOS_TREN.get(cod_product, f"Desconocido ({cod_product})"),
            tren.get("desCorridor"), tren.get("codOrigen"), tren.get("codDestino"),
            lat, lon, speed, bearing, retraso,
            tren.get("codEstAnt"), tren.get("codEstSig"),
            tren.get("horaLlegadaSigEst"), tren.get("p"), tren.get("mat"),
            1 if tren.get("accesible") else 0, gps_ts,
        ))
    return rows


def _process_largo_itinerarios(trenes_activos: set) -> list:
    """Guarda el itinerario de cada tren una sola vez por día."""
    _rollover_itineraries()
    today = _itin_today_date
    trenes_nuevos = {
        cod for cod in trenes_activos
        if (cod, today) not in _itin_today
    }
    if not trenes_nuevos:
        return []

    data = fetch_json(ENDPOINTS_LARGO["itinerarios"])
    now = datetime.now(timezone.utc).isoformat()

    rows = []
    for tren in data.get("trenes", []):
        cod = tren.get("idTren")
        if cod not in trenes_nuevos:
            continue
        coords = {
            s.get("c"): (s.get("lat"), s.get("lon"))
            for s in tren.get("secuencia", []) if s.get("c")
        }
        for order, est in enumerate(tren.get("estaciones", [])):
            code = est.get("p")
            lat, lon = coords.get(code, (None, None))
            rows.append((now, cod, order, code, est.get("h"), lat, lon))
        _itin_today.add((cod, today))

    return rows


# ╔══════════════════════════════════════════════════════════════╗
# ║  FLUSH A BASE DE DATOS                                       ║
# ╚══════════════════════════════════════════════════════════════╝

def flush_batch(conn, batch: dict):
    """Inserta en Azure SQL todas las filas acumuladas y vacía el batch."""
    cursor = conn.cursor()

    if batch["ast_snap"]:
        cursor.executemany(
            "INSERT INTO asturias_vehicle_snapshots "
            "(captured_at,feed_timestamp,trip_id,vehicle_id,vehicle_label,line,"
            "latitude,longitude,bearing,speed,current_status,stop_id,event_timestamp)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            batch["ast_snap"])

    if batch["ast_trips"]:
        cursor.executemany(
            "INSERT INTO asturias_trip_updates "
            "(captured_at,feed_timestamp,trip_id,line,schedule_relationship,"
            "stop_sequence,stop_id,arrival_delay,departure_delay)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            batch["ast_trips"])

    if batch["ast_alerts"]:
        cursor.executemany(
            "INSERT INTO asturias_service_alerts "
            "(captured_at,feed_timestamp,alert_id,cause,effect,header_text,description)"
            " VALUES (?,?,?,?,?,?,?)",
            batch["ast_alerts"])

    if batch["cdz_snap"]:
        cursor.executemany(
            "INSERT INTO cadiz_vehicle_snapshots "
            "(captured_at,feed_timestamp,trip_id,vehicle_id,vehicle_label,line,"
            "latitude,longitude,bearing,speed,current_status,stop_id,event_timestamp)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            batch["cdz_snap"])

    if batch["cdz_trips"]:
        cursor.executemany(
            "INSERT INTO cadiz_trip_updates "
            "(captured_at,feed_timestamp,trip_id,line,schedule_relationship,"
            "stop_sequence,stop_id,arrival_delay,departure_delay)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            batch["cdz_trips"])

    if batch["cdz_alerts"]:
        cursor.executemany(
            "INSERT INTO cadiz_service_alerts "
            "(captured_at,feed_timestamp,alert_id,cause,effect,header_text,description)"
            " VALUES (?,?,?,?,?,?,?)",
            batch["cdz_alerts"])

    if batch["lr_snap"]:
        cursor.executemany(
            "INSERT INTO train_snapshots "
            "(captured_at,feed_timestamp,cod_comercial,cod_product,tipo_tren,"
            "des_corridor,cod_origen,cod_destino,latitude,longitude,speed,bearing,"
            "ult_retraso,cod_est_ant,cod_est_sig,hora_llegada_sig_est,"
            "plataforma,material,accesible,gps_timestamp)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            batch["lr_snap"])

    if batch["lr_itin"]:
        cursor.executemany(
            "INSERT INTO train_itineraries "
            "(captured_at,cod_comercial,stop_order,station_code,hora_prog,latitude,longitude)"
            " VALUES (?,?,?,?,?,?,?)",
            batch["lr_itin"])

    conn.commit()
    cursor.close()

    total = sum(len(v) for v in batch.values())
    log.info(
        "FLUSH | AST: %d pos / %d upd | CDZ: %d pos / %d upd | "
        "LR: %d snap / %d itin | total %d filas",
        len(batch['ast_snap']), len(batch['ast_trips']),
        len(batch['cdz_snap']), len(batch['cdz_trips']),
        len(batch['lr_snap']),  len(batch['lr_itin']),
        total,
    )
    for key in batch:
        batch[key].clear()


def _empty_batch() -> dict:
    return {
        "ast_snap": [], "ast_trips": [], "ast_alerts": [],
        "cdz_snap": [], "cdz_trips": [], "cdz_alerts": [],
        "lr_snap":  [], "lr_itin":  [],
    }


# ╔══════════════════════════════════════════════════════════════╗
# ║  CAPTURA PRINCIPAL                                           ║
# ╚══════════════════════════════════════════════════════════════╝

def capture_once(batch: dict):
    stats = {k: 0 for k in batch}
    errors = []
    t0 = datetime.now()

    # ── Asturias ─────────────────────────────────────────────
    try:
        rows = _process_cerc_positions(ASTURIAS_BBOX, _ast_pos, _ast_first, _ast_trips)
        batch["ast_snap"].extend(rows)
        stats["ast_snap"] = len(rows)
    except Exception as e:
        errors.append(f"ast_pos: {e}")

    try:
        rows = _process_cerc_trip_updates(_ast_trips)
        batch["ast_trips"].extend(rows)
        stats["ast_trips"] = len(rows)
    except Exception as e:
        errors.append(f"ast_trips: {e}")

    try:
        rows = _process_cerc_alerts(ASTURIAS_ALERT_KEYWORDS)
        batch["ast_alerts"].extend(rows)
        stats["ast_alerts"] = len(rows)
    except Exception as e:
        errors.append(f"ast_alerts: {e}")

    # ── Cádiz ────────────────────────────────────────────────
    try:
        rows = _process_cerc_positions(CADIZ_BBOX, _cdz_pos, _cdz_first, _cdz_trips)
        batch["cdz_snap"].extend(rows)
        stats["cdz_snap"] = len(rows)
    except Exception as e:
        errors.append(f"cdz_pos: {e}")

    try:
        rows = _process_cerc_trip_updates(_cdz_trips)
        batch["cdz_trips"].extend(rows)
        stats["cdz_trips"] = len(rows)
    except Exception as e:
        errors.append(f"cdz_trips: {e}")

    try:
        rows = _process_cerc_alerts(CADIZ_ALERT_KEYWORDS)
        batch["cdz_alerts"].extend(rows)
        stats["cdz_alerts"] = len(rows)
    except Exception as e:
        errors.append(f"cdz_alerts: {e}")

    # ── Largo Recorrido ──────────────────────────────────────
    trenes_activos = set()
    try:
        rows = _process_largo_flota()
        batch["lr_snap"].extend(rows)
        stats["lr_snap"] = len(rows)
        trenes_activos = {r[2] for r in rows if r[2]}
    except Exception as e:
        errors.append(f"lr_flota: {e}")

    try:
        rows = _process_largo_itinerarios(trenes_activos)
        batch["lr_itin"].extend(rows)
        stats["lr_itin"] = len(rows)
    except Exception as e:
        errors.append(f"lr_itin: {e}")

    elapsed = (datetime.now() - t0).total_seconds()
    if errors:
        log.warning("CAPTURA | AST %dpos/%dupd  CDZ %dpos/%dupd  LR %dsnap/%ditin  %.1fs | ERR: %s",
            stats['ast_snap'], stats['ast_trips'],
            stats['cdz_snap'], stats['cdz_trips'],
            stats['lr_snap'],  stats['lr_itin'],
            elapsed, errors)
    else:
        log.info("CAPTURA | AST %dpos/%dupd  CDZ %dpos/%dupd  LR %dsnap/%ditin  %.1fs",
            stats['ast_snap'], stats['ast_trips'],
            stats['cdz_snap'], stats['cdz_trips'],
            stats['lr_snap'],  stats['lr_itin'],
            elapsed)


# ╔══════════════════════════════════════════════════════════════╗
# ║  SUMMARY                                                     ║
# ╚══════════════════════════════════════════════════════════════╝

def show_summary(conn):
    print("\n" + "=" * 65)
    print("  RESUMEN — CAPTURA UNIFICADA RENFE (Azure SQL)")
    print("=" * 65)
    cursor = conn.cursor()
    tables = [
        "asturias_vehicle_snapshots", "asturias_trip_updates", "asturias_service_alerts",
        "cadiz_vehicle_snapshots",    "cadiz_trip_updates",    "cadiz_service_alerts",
        "train_snapshots",            "train_itineraries",     "stations",
    ]
    for t in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {t}")
            print(f"  {t}: {cursor.fetchone()[0]:,} registros")
        except Exception:
            print(f"  {t}: (no existe)")

    for prefix, table in [("Asturias", "asturias_vehicle_snapshots"),
                           ("Cádiz",   "cadiz_vehicle_snapshots")]:
        cursor.execute(f"""
            SELECT line, ROUND(AVG(speed),1), COUNT(*)
            FROM {table} WHERE speed IS NOT NULL AND line IS NOT NULL
            GROUP BY line ORDER BY AVG(speed) DESC
        """)
        rows = cursor.fetchall()
        if rows:
            print(f"\n  Velocidades {prefix}:")
            for line, avg_s, cnt in rows:
                print(f"    {line}: {avg_s} km/h media ({cnt} registros)")

    cursor.execute("""
        SELECT tipo_tren, ROUND(AVG(CAST(ult_retraso AS FLOAT)),1), COUNT(*)
        FROM train_snapshots WHERE ult_retraso IS NOT NULL
        GROUP BY tipo_tren ORDER BY COUNT(*) DESC
    """)
    rows = cursor.fetchall()
    if rows:
        print("\n  Retrasos largo recorrido (min):")
        for tipo, avg_r, cnt in rows:
            print(f"    {tipo}: {avg_r} min media ({cnt} registros)")

    cursor.execute("SELECT MIN(captured_at), MAX(captured_at) FROM asturias_vehicle_snapshots")
    row = cursor.fetchone()
    if row[0]:
        print(f"\n  Período: {row[0][:19]} → {row[1][:19]}")
    cursor.close()
    print("=" * 65 + "\n")


# ╔══════════════════════════════════════════════════════════════╗
# ║  INIT STATIONS                                               ║
# ╚══════════════════════════════════════════════════════════════╝

def init_stations(conn):
    data = fetch_json(ENDPOINTS_LARGO["estaciones"])
    rows = []
    for feat in data.get("features", []):
        p = feat.get("properties", {})
        rows.append((
            p.get("CODIGO"), p.get("NOMBRE"), p.get("LAT"), p.get("LON"),
            p.get("ACCESIBLE"), p.get("DIR"), p.get("LOCALIDAD"), p.get("PROV"),
            1 if p.get("CERC") else 0, p.get("NIVEL"),
        ))
    if rows:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM stations")
        cursor.executemany(
            "INSERT INTO stations (codigo,nombre,latitude,longitude,accesible,"
            "direccion,localidad,provincia,es_cercanias,nivel) VALUES (?,?,?,?,?,?,?,?,?,?)",
            rows)
        conn.commit()
        cursor.close()
    print(f"Cargadas {len(rows)} estaciones en Azure SQL.")


# ╔══════════════════════════════════════════════════════════════╗
# ║  MAIN                                                        ║
# ╚══════════════════════════════════════════════════════════════╝

def main():
    parser = argparse.ArgumentParser(
        description="Captura unificada Renfe → Azure SQL"
    )
    parser.add_argument("--loop", type=int, default=0,
                        help="Intervalo en segundos (0 = captura única)")
    parser.add_argument("--summary",       action="store_true")
    parser.add_argument("--init-stations", action="store_true")
    parser.add_argument("--flush-every",   type=int, default=FLUSH_CYCLES,
                        help=f"Flush a BD cada N ciclos (default: {FLUSH_CYCLES} → 20 min con --loop 30)")
    args = parser.parse_args()

    conn = get_conn()
    init_db(conn)
    conn.close()

    if args.init_stations:
        conn = get_conn()
        init_stations(conn)
        conn.close()

    if args.summary:
        conn = get_conn()
        show_summary(conn)
        conn.close()
        return

    if args.loop > 0:
        flush_every = args.flush_every
        print(
            f"Captura cada {args.loop}s | "
            f"Flush cada {flush_every * args.loop}s | "
            f"Pausa nocturna {NIGHT_START:02d}:00–{NIGHT_END:02d}:00 UTC\n"
        )
        batch = _empty_batch()
        cycle = 0
        try:
            while True:
                result = wait_if_night(batch)
                if result == 0:
                    cycle = 0  # se acaba de despertar tras pausa nocturna
                capture_once(batch)
                cycle += 1
                if cycle >= flush_every:
                    conn = get_conn()
                    flush_batch(conn, batch)
                    conn.close()
                    cycle = 0
                time.sleep(args.loop)
        except KeyboardInterrupt:
            print("\nDetenido. Volcando datos pendientes...")
            if any(batch[k] for k in batch):
                conn = get_conn()
                flush_batch(conn, batch)
                conn.close()
            conn = get_conn()
            show_summary(conn)
            conn.close()
    else:
        batch = _empty_batch()
        capture_once(batch)
        conn = get_conn()
        flush_batch(conn, batch)
        conn.close()


if __name__ == "__main__":
    main()
