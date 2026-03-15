"""
Captura de datos en tiempo real de Cercanías Asturias (GTFS-RT Renfe)
=====================================================================
Fuentes oficiales (CC BY 4.0):
  - Posiciones:   https://gtfsrt.renfe.com/vehicle_positions.json
  - Trip updates: https://gtfsrt.renfe.com/trip_updates.json
  - Alertas:      https://gtfsrt.renfe.com/alerts.json

Uso:
  python renfe_asturias_cercanias.py                  # Captura única
  python renfe_asturias_cercanias.py --loop 30        # Captura cada 30s, flush cada 5 min
  python renfe_asturias_cercanias.py --summary        # Ver resumen
"""

import math
import re
import requests
import argparse
import time
from datetime import datetime, timezone

import pyodbc
from azure_db import get_conn, run_ddl

# ╔══════════════════════════════════════════════════════════════╗
# ║  CONFIGURACIÓN                                               ║
# ╚══════════════════════════════════════════════════════════════╝

ASTURIAS_BBOX = {
    "lat_min": 43.0,
    "lat_max": 43.7,
    "lon_min": -7.0,
    "lon_max": -4.5,
}

ENDPOINTS = {
    "vehicle_positions": "https://gtfsrt.renfe.com/vehicle_positions.json",
    "trip_updates":      "https://gtfsrt.renfe.com/trip_updates.json",
    "service_alerts":    "https://gtfsrt.renfe.com/alerts.json",
}

REQUEST_TIMEOUT = 15
FLUSH_CYCLES = 10  # 10 ciclos × 30s = 5 minutos entre cada flush a la BD

# ╔══════════════════════════════════════════════════════════════╗
# ║  BASE DE DATOS                                               ║
# ╚══════════════════════════════════════════════════════════════╝

def init_db(conn: pyodbc.Connection):
    """Crea tablas, índices y vistas si no existen (Azure SQL)."""
    run_ddl(conn, [
        """
        IF OBJECT_ID('asturias_vehicle_snapshots', 'U') IS NULL
        CREATE TABLE asturias_vehicle_snapshots (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            captured_at     NVARCHAR(50)  NOT NULL,
            feed_timestamp  BIGINT,
            trip_id         NVARCHAR(100),
            vehicle_id      NVARCHAR(50),
            vehicle_label   NVARCHAR(200),
            line            NVARCHAR(20),
            latitude        FLOAT,
            longitude       FLOAT,
            bearing         FLOAT,
            speed           FLOAT,
            current_status  NVARCHAR(50),
            stop_id         NVARCHAR(50),
            event_timestamp BIGINT
        )
        """,
        """
        IF OBJECT_ID('asturias_trip_updates', 'U') IS NULL
        CREATE TABLE asturias_trip_updates (
            id                      INT IDENTITY(1,1) PRIMARY KEY,
            captured_at             NVARCHAR(50)  NOT NULL,
            feed_timestamp          BIGINT,
            trip_id                 NVARCHAR(100),
            line                    NVARCHAR(20),
            schedule_relationship   NVARCHAR(50),
            stop_sequence           INT,
            stop_id                 NVARCHAR(50),
            arrival_delay           INT,
            departure_delay         INT
        )
        """,
        """
        IF OBJECT_ID('asturias_service_alerts', 'U') IS NULL
        CREATE TABLE asturias_service_alerts (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            captured_at     NVARCHAR(50)  NOT NULL,
            feed_timestamp  BIGINT,
            alert_id        NVARCHAR(100),
            cause           NVARCHAR(100),
            effect          NVARCHAR(100),
            header_text     NVARCHAR(MAX),
            description     NVARCHAR(MAX)
        )
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name='idx_ast_vs_captured'
                         AND object_id=OBJECT_ID('asturias_vehicle_snapshots'))
        CREATE INDEX idx_ast_vs_captured ON asturias_vehicle_snapshots(captured_at)
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name='idx_ast_vs_line'
                         AND object_id=OBJECT_ID('asturias_vehicle_snapshots'))
        CREATE INDEX idx_ast_vs_line ON asturias_vehicle_snapshots(line)
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name='idx_ast_vs_vehicle_ts'
                         AND object_id=OBJECT_ID('asturias_vehicle_snapshots'))
        CREATE INDEX idx_ast_vs_vehicle_ts ON asturias_vehicle_snapshots(vehicle_id, event_timestamp DESC)
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name='idx_ast_tu_trip'
                         AND object_id=OBJECT_ID('asturias_trip_updates'))
        CREATE INDEX idx_ast_tu_trip ON asturias_trip_updates(trip_id)
        """,
    ])

    cursor = conn.cursor()
    for tbl in ["asturias_vehicle_snapshots", "asturias_trip_updates", "asturias_service_alerts"]:
        cursor.execute(
            f"IF COL_LENGTH('{tbl}', 'raw_json') IS NOT NULL "
            f"ALTER TABLE {tbl} DROP COLUMN raw_json"
        )
    conn.commit()
    cursor.close()
    print("Tablas e índices verificados (Azure SQL — Asturias).")


# ╔══════════════════════════════════════════════════════════════╗
# ║  CÁLCULO DE VELOCIDAD Y BEARING                              ║
# ╚══════════════════════════════════════════════════════════════╝

MIN_SPEED_KMH = 0.5
MAX_SPEED_KMH = 200.0


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


def compute_speed_bearing(lat1, lon1, ts1, lat2, lon2, ts2):
    if None in (lat1, lon1, ts1, lat2, lon2, ts2):
        return None, None
    dt = int(ts2) - int(ts1)
    if dt <= 0:
        return None, None
    dist_km = haversine_km(lat1, lon1, lat2, lon2)
    speed_kmh = dist_km / (dt / 3600)
    if speed_kmh < MIN_SPEED_KMH or speed_kmh > MAX_SPEED_KMH:
        return None, None
    return round(speed_kmh, 2), round(calculate_bearing(lat1, lon1, lat2, lon2), 1)


# ╔══════════════════════════════════════════════════════════════╗
# ║  CACHÉS EN MEMORIA (evitan consultas DB entre flushes)       ║
# ╚══════════════════════════════════════════════════════════════╝

# vehicle_id -> (lat, lon, event_ts, captured_at_iso)
_position_cache: dict = {}

# (vehicle_id, lat_redondeada, lon_redondeada) -> primera_captured_at_iso en esa posición
_first_pos_cache: dict = {}

# trip_ids vistos en la sesión actual
_known_trips: set = set()


def _get_previous_snapshot(vehicle_id):
    """Devuelve el último snapshot conocido del vehículo (desde caché en memoria)."""
    if not vehicle_id:
        return None
    return _position_cache.get(vehicle_id)


def _get_first_position_captured_at(vehicle_id, lat, lon):
    """
    Devuelve el ISO timestamp de la primera vez que vimos este vehículo
    en las coordenadas (lat, lon), desde el caché en memoria.
    Compensa la baja frecuencia GPS de Renfe en cercanías.
    """
    key = (vehicle_id, round(lat, 4), round(lon, 4))
    return _first_pos_cache.get(key)


# ╔══════════════════════════════════════════════════════════════╗
# ║  FETCH Y PROCESAMIENTO (acumulan en batch, sin acceso a BD)  ║
# ╚══════════════════════════════════════════════════════════════╝

def fetch_json(url):
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def is_in_asturias(lat, lon):
    return (ASTURIAS_BBOX["lat_min"] <= lat <= ASTURIAS_BBOX["lat_max"]
            and ASTURIAS_BBOX["lon_min"] <= lon <= ASTURIAS_BBOX["lon_max"])


def extract_line_from_label(label):
    if not label:
        return None
    return label.split("-")[0] if "-" in label else None


def process_vehicle_positions() -> list:
    """Captura posiciones, actualiza cachés y devuelve filas (sin insertar en BD)."""
    data = fetch_json(ENDPOINTS["vehicle_positions"])
    feed_ts = data.get("header", {}).get("timestamp")
    now = datetime.now(timezone.utc).isoformat()

    rows = []
    for entity in data.get("entity", []):
        v = entity.get("vehicle", {})
        pos = v.get("position", {})
        lat = pos.get("latitude")
        lon = pos.get("longitude")
        if lat is None or lon is None or not is_in_asturias(lat, lon):
            continue

        veh = v.get("vehicle", {})
        vehicle_id = veh.get("id")
        event_ts = v.get("timestamp")

        speed, bearing = None, None
        prev = _get_previous_snapshot(vehicle_id)
        if prev:
            prev_lat, prev_lon, _, _prev_captured_at = prev
            pos_changed = (abs(lat - prev_lat) > 0.0001 or abs(lon - prev_lon) > 0.0001)
            if pos_changed:
                first_at_iso = _get_first_position_captured_at(vehicle_id, prev_lat, prev_lon)
                if first_at_iso is not None:
                    try:
                        ts1 = datetime.fromisoformat(first_at_iso).timestamp()
                    except Exception:
                        ts1 = None
                else:
                    try:
                        ts1 = datetime.fromisoformat(_prev_captured_at).timestamp()
                    except Exception:
                        ts1 = None
                ts2 = datetime.fromisoformat(now).timestamp()
                speed, bearing = compute_speed_bearing(prev_lat, prev_lon, ts1, lat, lon, ts2)

                # Registrar primera vez en la nueva posición
                new_key = (vehicle_id, round(lat, 4), round(lon, 4))
                if new_key not in _first_pos_cache:
                    _first_pos_cache[new_key] = now

        # Registrar primera vez que vemos este vehículo en esta posición
        if vehicle_id:
            pos_key = (vehicle_id, round(lat, 4), round(lon, 4))
            if pos_key not in _first_pos_cache:
                _first_pos_cache[pos_key] = now
            _position_cache[vehicle_id] = (lat, lon, event_ts, now)

        label = veh.get("label", "")
        rows.append((
            now, feed_ts,
            v.get("trip", {}).get("tripId"),
            vehicle_id, label,
            extract_line_from_label(label),
            lat, lon, bearing, speed,
            v.get("currentStatus"),
            v.get("stopId"), event_ts,
        ))

    return rows


def process_trip_updates() -> list:
    """Captura trip updates filtrando por _known_trips y devuelve filas (sin insertar)."""
    if not _known_trips:
        return []
    data = fetch_json(ENDPOINTS["trip_updates"])
    feed_ts = data.get("header", {}).get("timestamp")
    now = datetime.now(timezone.utc).isoformat()

    rows = []
    for entity in data.get("entity", []):
        tu = entity.get("tripUpdate", {})
        trip = tu.get("trip", {})
        trip_id = trip.get("tripId")
        if trip_id not in _known_trips:
            continue

        schedule_rel = trip.get("scheduleRelationship")
        stop_time_updates = tu.get("stopTimeUpdate", [])

        if not stop_time_updates:
            rows.append((now, feed_ts, trip_id, None, schedule_rel,
                         None, None, None, None))
        else:
            for stu in stop_time_updates:
                line = None
                if trip_id:
                    parts = trip_id.split("L")
                    if len(parts) == 2:
                        m = re.search(r'([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*)$', parts[1])
                        line = m.group(1) if m else None
                arrival = stu.get("arrival", {})
                departure = stu.get("departure", {})
                rows.append((
                    now, feed_ts, trip_id, line, schedule_rel,
                    stu.get("stopSequence"), stu.get("stopId"),
                    arrival.get("delay"), departure.get("delay"),
                ))

    return rows


def process_service_alerts() -> list:
    """Captura alertas de Asturias y devuelve filas (sin insertar)."""
    data = fetch_json(ENDPOINTS["service_alerts"])
    feed_ts = data.get("header", {}).get("timestamp")
    now = datetime.now(timezone.utc).isoformat()

    asturias_keywords = ["asturias", "oviedo", "gijón", "gijon", "avilés", "aviles",
                         "mieres", "langreo", "laviana", "el entrego", "pola de lena",
                         "la corredoria", "llamaquique", "feve"]
    rows = []
    for entity in data.get("entity", []):
        alert = entity.get("alert", {})
        header_tr = alert.get("headerText", {}).get("translation", [])
        desc_tr = alert.get("descriptionText", {}).get("translation", [])
        header = header_tr[0].get("text", "") if header_tr else ""
        description = desc_tr[0].get("text", "") if desc_tr else ""
        if not any(kw in (header + " " + description).lower() for kw in asturias_keywords):
            continue
        rows.append((now, feed_ts, entity.get("id"),
                     alert.get("cause"), alert.get("effect"),
                     header, description))

    return rows


# ╔══════════════════════════════════════════════════════════════╗
# ║  FLUSH A BASE DE DATOS                                       ║
# ╚══════════════════════════════════════════════════════════════╝

def flush_batch(conn, batch: dict):
    """Inserta en Azure SQL todas las filas acumuladas en el batch y lo vacía."""
    cursor = conn.cursor()

    if batch["snapshots"]:
        cursor.executemany("""
            INSERT INTO asturias_vehicle_snapshots
            (captured_at, feed_timestamp, trip_id, vehicle_id, vehicle_label,
             line, latitude, longitude, bearing, speed, current_status,
             stop_id, event_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch["snapshots"])

    if batch["trip_updates"]:
        cursor.executemany("""
            INSERT INTO asturias_trip_updates
            (captured_at, feed_timestamp, trip_id, line, schedule_relationship,
             stop_sequence, stop_id, arrival_delay, departure_delay)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch["trip_updates"])

    if batch["alerts"]:
        cursor.executemany("""
            INSERT INTO asturias_service_alerts
            (captured_at, feed_timestamp, alert_id, cause, effect,
             header_text, description)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, batch["alerts"])

    conn.commit()
    cursor.close()

    print(
        f"  → Flush BD: {len(batch['snapshots'])} posiciones | "
        f"{len(batch['trip_updates'])} trip updates | "
        f"{len(batch['alerts'])} alertas"
    )
    batch["snapshots"].clear()
    batch["trip_updates"].clear()
    batch["alerts"].clear()


# ╔══════════════════════════════════════════════════════════════╗
# ║  CAPTURA PRINCIPAL                                           ║
# ╚══════════════════════════════════════════════════════════════╝

def capture_once(batch: dict):
    """Captura un ciclo y acumula las filas en batch (sin acceder a la BD)."""
    stats = {"positions": 0, "trip_updates": 0, "alerts": 0, "errors": []}
    now = datetime.now()

    try:
        rows = process_vehicle_positions()
        batch["snapshots"].extend(rows)
        stats["positions"] = len(rows)
        for row in rows:
            if row[2]:  # trip_id
                _known_trips.add(row[2])
    except Exception as e:
        stats["errors"].append(f"vehicle_positions: {e}")

    try:
        rows = process_trip_updates()
        batch["trip_updates"].extend(rows)
        stats["trip_updates"] = len(rows)
    except Exception as e:
        stats["errors"].append(f"trip_updates: {e}")

    try:
        rows = process_service_alerts()
        batch["alerts"].extend(rows)
        stats["alerts"] = len(rows)
    except Exception as e:
        stats["errors"].append(f"service_alerts: {e}")

    elapsed = (datetime.now() - now).total_seconds()
    err_str = f" | ERRORES: {stats['errors']}" if stats["errors"] else ""
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
        f"Posiciones: {stats['positions']} | "
        f"Trip Updates: {stats['trip_updates']} | "
        f"Alertas: {stats['alerts']} | "
        f"{elapsed:.1f}s{err_str}"
    )
    return stats


def show_summary(conn):
    print("\n" + "=" * 60)
    print("  RESUMEN — CERCANÍAS ASTURIAS (Azure SQL)")
    print("=" * 60)

    cursor = conn.cursor()
    for table in ["asturias_vehicle_snapshots", "asturias_trip_updates", "asturias_service_alerts"]:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table}: {cursor.fetchone()[0]:,} registros")

    cursor.execute("""
        SELECT line, COUNT(*) as cnt, COUNT(DISTINCT trip_id) as trips
        FROM asturias_vehicle_snapshots WHERE line IS NOT NULL
        GROUP BY line ORDER BY cnt DESC
    """)
    rows = cursor.fetchall()
    if rows:
        print("\n  Líneas detectadas:")
        for line, cnt, trips in rows:
            print(f"    {line}: {cnt:,} snapshots, {trips} viajes")

    cursor.execute("""
        SELECT line, ROUND(AVG(speed), 1), ROUND(MAX(speed), 1), COUNT(*)
        FROM asturias_vehicle_snapshots
        WHERE speed IS NOT NULL AND line IS NOT NULL
        GROUP BY line ORDER BY AVG(speed) DESC
    """)
    rows = cursor.fetchall()
    if rows:
        print("\n  Velocidad por línea (km/h):")
        for line, avg_s, max_s, cnt in rows:
            print(f"    {line}: media {avg_s} km/h, máx {max_s} km/h ({cnt} registros)")

    cursor.execute("""
        SELECT line, ROUND(AVG(CAST(arrival_delay AS FLOAT)), 1), MAX(arrival_delay), COUNT(*)
        FROM asturias_trip_updates
        WHERE arrival_delay IS NOT NULL AND line IS NOT NULL
        GROUP BY line
    """)
    rows = cursor.fetchall()
    if rows:
        print("\n  Retrasos por línea:")
        for line, avg_d, max_d, cnt in rows:
            print(f"    {line}: media {avg_d}s, máx {max_d}s ({cnt} registros)")

    cursor.execute("SELECT MIN(captured_at), MAX(captured_at) FROM asturias_vehicle_snapshots")
    row = cursor.fetchone()
    if row[0]:
        print(f"\n  Período: {row[0][:19]} → {row[1][:19]}")
    cursor.close()
    print("=" * 60 + "\n")


# ╔══════════════════════════════════════════════════════════════╗
# ║  MAIN                                                        ║
# ╚══════════════════════════════════════════════════════════════╝

def main():
    parser = argparse.ArgumentParser(
        description="Captura Cercanías Asturias → Azure SQL"
    )
    parser.add_argument("--loop", type=int, default=0,
                        help="Intervalo en segundos (0 = captura única)")
    parser.add_argument("--summary", action="store_true")
    parser.add_argument("--flush-every", type=int, default=FLUSH_CYCLES,
                        help="Flush a BD cada N ciclos (default: 10 → 5 min con --loop 30)")
    args = parser.parse_args()

    conn = get_conn()
    init_db(conn)
    conn.close()

    if args.summary:
        conn = get_conn()
        show_summary(conn)
        conn.close()
        return

    if args.loop > 0:
        flush_every = args.flush_every
        print(
            f"Modo loop: captura cada {args.loop}s, "
            f"flush a BD cada {flush_every * args.loop}s "
            f"(Ctrl+C para parar)\n"
        )
        batch = {"snapshots": [], "trip_updates": [], "alerts": []}
        cycle = 0
        try:
            while True:
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
        batch = {"snapshots": [], "trip_updates": [], "alerts": []}
        capture_once(batch)
        conn = get_conn()
        flush_batch(conn, batch)
        conn.close()


if __name__ == "__main__":
    main()
