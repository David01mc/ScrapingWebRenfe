"""
Captura de datos en tiempo real — Largo Recorrido Renfe (Cádiz ↔ Madrid)
=========================================================================
Fuentes oficiales (datos públicos):
  - Posiciones:  https://tiempo-real.largorecorrido.renfe.com/renfe-visor/flotaLD.json
  - Itinerarios: https://tiempo-real.largorecorrido.renfe.com/renfe-visor/trenesConEstacionesLD.json
  - Estaciones:  https://tiempo-real.largorecorrido.renfe.com/data/estaciones.geojson

El feed se actualiza cada 15 segundos. Capturamos cada 30s.

Uso:
  python renfe_largo_recorrido.py                  # Captura única
  python renfe_largo_recorrido.py --loop 30        # Captura cada 30s, flush cada 5 min
  python renfe_largo_recorrido.py --summary        # Ver resumen
  python renfe_largo_recorrido.py --init-stations  # Cargar catálogo de estaciones (una vez)
"""

import math
import requests
import argparse
import time
from datetime import datetime, timezone

import pyodbc
from azure_db import get_conn, run_ddl

# ╔══════════════════════════════════════════════════════════════╗
# ║  CONFIGURACIÓN                                               ║
# ╚══════════════════════════════════════════════════════════════╝

BASE_URL = "https://tiempo-real.largorecorrido.renfe.com"

ENDPOINTS = {
    "flota":      f"{BASE_URL}/renfe-visor/flotaLD.json",
    "itinerarios": f"{BASE_URL}/renfe-visor/trenesConEstacionesLD.json",
    "estaciones": f"{BASE_URL}/data/estaciones.geojson",
}

REQUEST_TIMEOUT = 15
CADIZ_MADRID_KEYWORDS = ["ádiz", "adiz"]
FLUSH_CYCLES = 10  # 10 ciclos × 30s = 5 minutos entre cada flush a la BD

TIPOS_TREN = {
    1:  "AVE",
    2:  "Larga Distancia",
    3:  "Avant",
    4:  "Alvia",
    5:  "Alaris",
    6:  "Altaria",
    7:  "Arco",
    8:  "Euromed",
    9:  "Talgo",
    10: "Intercity",
    11: "Media Distancia",
    12: "Regional",
    13: "Regional Exprés",
}

# ╔══════════════════════════════════════════════════════════════╗
# ║  BASE DE DATOS                                               ║
# ╚══════════════════════════════════════════════════════════════╝

def init_db(conn: pyodbc.Connection):
    """Crea tablas e índices si no existen (Azure SQL)."""
    run_ddl(conn, [
        """
        IF OBJECT_ID('train_snapshots', 'U') IS NULL
        CREATE TABLE train_snapshots (
            id                      INT IDENTITY(1,1) PRIMARY KEY,
            captured_at             NVARCHAR(50)  NOT NULL,
            feed_timestamp          NVARCHAR(50),
            cod_comercial           NVARCHAR(20),
            cod_product             INT,
            tipo_tren               NVARCHAR(50),
            des_corridor            NVARCHAR(200),
            cod_origen              NVARCHAR(20),
            cod_destino             NVARCHAR(20),
            latitude                FLOAT,
            longitude               FLOAT,
            speed                   FLOAT,
            bearing                 FLOAT,
            ult_retraso             INT,
            cod_est_ant             NVARCHAR(20),
            cod_est_sig             NVARCHAR(20),
            hora_llegada_sig_est    NVARCHAR(20),
            plataforma              NVARCHAR(20),
            material                NVARCHAR(50),
            accesible               INT,
            gps_timestamp           BIGINT
        )
        """,
        """
        IF OBJECT_ID('train_itineraries', 'U') IS NULL
        CREATE TABLE train_itineraries (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            captured_at     NVARCHAR(50)  NOT NULL,
            cod_comercial   NVARCHAR(20),
            stop_order      INT,
            station_code    NVARCHAR(20),
            hora_prog       NVARCHAR(10),
            latitude        FLOAT,
            longitude       FLOAT
        )
        """,
        """
        IF OBJECT_ID('stations', 'U') IS NULL
        CREATE TABLE stations (
            codigo      INT PRIMARY KEY,
            nombre      NVARCHAR(200),
            latitude    FLOAT,
            longitude   FLOAT,
            accesible   INT,
            direccion   NVARCHAR(300),
            localidad   NVARCHAR(100),
            provincia   NVARCHAR(100),
            es_cercanias INT,
            nivel       INT
        )
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name='idx_ts_captured'
                         AND object_id=OBJECT_ID('train_snapshots'))
        CREATE INDEX idx_ts_captured ON train_snapshots(captured_at)
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name='idx_ts_tren'
                         AND object_id=OBJECT_ID('train_snapshots'))
        CREATE INDEX idx_ts_tren ON train_snapshots(cod_comercial)
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name='idx_ts_gps'
                         AND object_id=OBJECT_ID('train_snapshots'))
        CREATE INDEX idx_ts_gps ON train_snapshots(cod_comercial, gps_timestamp DESC)
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name='idx_ti_tren'
                         AND object_id=OBJECT_ID('train_itineraries'))
        CREATE INDEX idx_ti_tren ON train_itineraries(cod_comercial)
        """,
    ])

    cursor = conn.cursor()
    for tbl in ["train_snapshots", "train_itineraries", "stations"]:
        cursor.execute(
            f"IF COL_LENGTH('{tbl}', 'raw_json') IS NOT NULL "
            f"ALTER TABLE {tbl} DROP COLUMN raw_json"
        )
    conn.commit()
    cursor.close()
    print("Tablas e índices verificados (Azure SQL — Largo Recorrido).")


# ╔══════════════════════════════════════════════════════════════╗
# ║  CÁLCULO DE VELOCIDAD Y BEARING                              ║
# ╚══════════════════════════════════════════════════════════════╝

MIN_SPEED_KMH = 0.5
MAX_SPEED_KMH = 350.0  # AVE puede ir hasta ~310 km/h


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
# ║  CACHÉ EN MEMORIA (evita consultas DB entre flushes)         ║
# ╚══════════════════════════════════════════════════════════════╝

# cod_comercial -> (lat, lon, gps_timestamp, captured_at_iso)
_position_cache: dict = {}


def _get_previous_snapshot(cod_comercial):
    """Devuelve el último snapshot conocido del tren (desde caché en memoria)."""
    if not cod_comercial:
        return None
    return _position_cache.get(cod_comercial)


def _resolve_timestamps(prev_gps_ts, prev_captured_at, curr_gps_ts, curr_captured_at):
    try:
        if (prev_gps_ts is not None and curr_gps_ts is not None
                and int(curr_gps_ts) != int(prev_gps_ts)):
            return int(prev_gps_ts), int(curr_gps_ts)
        prev_ts = datetime.fromisoformat(prev_captured_at).timestamp()
        curr_ts = datetime.fromisoformat(curr_captured_at).timestamp()
        return prev_ts, curr_ts
    except Exception:
        return None, None


# ╔══════════════════════════════════════════════════════════════╗
# ║  FETCH DE DATOS                                              ║
# ╚══════════════════════════════════════════════════════════════╝

def fetch_json(url):
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def is_cadiz_madrid(tren: dict) -> bool:
    corridor = (tren.get("desCorridor") or "").lower()
    return any(kw in corridor for kw in CADIZ_MADRID_KEYWORDS)


# ╔══════════════════════════════════════════════════════════════╗
# ║  PROCESAMIENTO (acumulan en batch, sin acceso a BD)          ║
# ╚══════════════════════════════════════════════════════════════╝

def process_flota() -> list:
    """Captura posiciones y retrasos de trenes Cádiz ↔ Madrid. Devuelve filas sin insertar."""
    data = fetch_json(ENDPOINTS["flota"])
    feed_ts = data.get("fechaActualizacion")
    now = datetime.now(timezone.utc).isoformat()

    trenes = [t for t in data.get("trenes", []) if is_cadiz_madrid(t)]

    rows = []
    for tren in trenes:
        cod = tren.get("codComercial")
        lat = tren.get("latitud")
        lon = tren.get("longitud")
        gps_ts = tren.get("time")
        cod_product = tren.get("codProduct")

        speed, bearing = None, None
        prev = _get_previous_snapshot(cod)
        if prev:
            prev_lat, prev_lon, prev_gps_ts, prev_captured_at = prev
            ts1, ts2 = _resolve_timestamps(prev_gps_ts, prev_captured_at, gps_ts, now)
            speed, bearing = compute_speed_bearing(prev_lat, prev_lon, ts1, lat, lon, ts2)

        # Actualizar caché
        if cod:
            _position_cache[cod] = (lat, lon, gps_ts, now)

        retraso_raw = tren.get("ultRetraso", "0")
        try:
            retraso = int(retraso_raw)
        except (ValueError, TypeError):
            retraso = None

        rows.append((
            now, feed_ts, cod, cod_product,
            TIPOS_TREN.get(cod_product, f"Desconocido ({cod_product})"),
            tren.get("desCorridor"),
            tren.get("codOrigen"),
            tren.get("codDestino"),
            lat, lon, speed, bearing, retraso,
            tren.get("codEstAnt"),
            tren.get("codEstSig"),
            tren.get("horaLlegadaSigEst"),
            tren.get("p"),
            tren.get("mat"),
            1 if tren.get("accesible") else 0,
            gps_ts,
        ))

    return rows


def process_itinerarios(trenes_activos: set) -> list:
    """Captura itinerarios completos de trenes activos. Devuelve filas sin insertar."""
    if not trenes_activos:
        return []

    data = fetch_json(ENDPOINTS["itinerarios"])
    now = datetime.now(timezone.utc).isoformat()

    rows = []
    for tren in data.get("trenes", []):
        cod = tren.get("idTren")
        if cod not in trenes_activos:
            continue

        coords_por_estacion = {
            s.get("c"): (s.get("lat"), s.get("lon"))
            for s in tren.get("secuencia", []) if s.get("c")
        }

        for order, est in enumerate(tren.get("estaciones", [])):
            code = est.get("p")
            lat, lon = coords_por_estacion.get(code, (None, None))
            rows.append((
                now, cod, order, code,
                est.get("h"),
                lat, lon,
            ))

    return rows


def init_stations(conn) -> int:
    """Carga el catálogo completo de estaciones (operación única)."""
    data = fetch_json(ENDPOINTS["estaciones"])
    features = data.get("features", [])

    rows = []
    for feat in features:
        props = feat.get("properties", {})
        rows.append((
            props.get("CODIGO"),
            props.get("NOMBRE"),
            props.get("LAT"),
            props.get("LON"),
            props.get("ACCESIBLE"),
            props.get("DIR"),
            props.get("LOCALIDAD"),
            props.get("PROV"),
            1 if props.get("CERC") else 0,
            props.get("NIVEL"),
        ))

    if rows:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM stations")
        cursor.executemany("""
            INSERT INTO stations
            (codigo, nombre, latitude, longitude, accesible,
             direccion, localidad, provincia, es_cercanias, nivel)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        cursor.close()

    print(f"Cargadas {len(rows)} estaciones en Azure SQL.")
    return len(rows)


# ╔══════════════════════════════════════════════════════════════╗
# ║  FLUSH A BASE DE DATOS                                       ║
# ╚══════════════════════════════════════════════════════════════╝

def flush_batch(conn, batch: dict):
    """Inserta en Azure SQL todas las filas acumuladas en el batch y lo vacía."""
    cursor = conn.cursor()

    if batch["snapshots"]:
        cursor.executemany("""
            INSERT INTO train_snapshots
            (captured_at, feed_timestamp, cod_comercial, cod_product, tipo_tren,
             des_corridor, cod_origen, cod_destino, latitude, longitude,
             speed, bearing, ult_retraso, cod_est_ant, cod_est_sig,
             hora_llegada_sig_est, plataforma, material, accesible,
             gps_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch["snapshots"])

    if batch["itineraries"]:
        cursor.executemany("""
            INSERT INTO train_itineraries
            (captured_at, cod_comercial, stop_order, station_code,
             hora_prog, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, batch["itineraries"])

    conn.commit()
    cursor.close()

    print(
        f"  → Flush BD: {len(batch['snapshots'])} snapshots | "
        f"{len(batch['itineraries'])} paradas"
    )
    batch["snapshots"].clear()
    batch["itineraries"].clear()


# ╔══════════════════════════════════════════════════════════════╗
# ║  CAPTURA PRINCIPAL                                           ║
# ╚══════════════════════════════════════════════════════════════╝

def capture_once(batch: dict) -> dict:
    """Captura un ciclo y acumula las filas en batch (sin acceder a la BD)."""
    stats = {"trenes": 0, "paradas": 0, "errors": []}
    now = datetime.now()

    trenes_activos = set()
    try:
        rows = process_flota()
        batch["snapshots"].extend(rows)
        stats["trenes"] = len(rows)
        trenes_activos = {row[2] for row in rows if row[2]}  # cod_comercial
    except Exception as e:
        stats["errors"].append(f"flota: {e}")

    try:
        rows = process_itinerarios(trenes_activos)
        batch["itineraries"].extend(rows)
        stats["paradas"] = len(rows)
    except Exception as e:
        stats["errors"].append(f"itinerarios: {e}")

    elapsed = (datetime.now() - now).total_seconds()
    err_str = f" | ERRORES: {stats['errors']}" if stats["errors"] else ""
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
        f"Trenes Cádiz↔Madrid: {stats['trenes']} | "
        f"Paradas: {stats['paradas']} | "
        f"{elapsed:.1f}s{err_str}"
    )
    return stats


def show_summary(conn):
    print("\n" + "=" * 60)
    print("  RESUMEN — LARGO RECORRIDO CÁDIZ ↔ MADRID (Azure SQL)")
    print("=" * 60)

    cursor = conn.cursor()
    for table in ["train_snapshots", "train_itineraries", "stations"]:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table}: {cursor.fetchone()[0]:,} registros")

    cursor.execute("""
        SELECT
            cod_comercial, tipo_tren, des_corridor,
            COUNT(*) as snapshots,
            ROUND(AVG(CAST(ult_retraso AS FLOAT)), 1) as retraso_medio,
            MAX(ult_retraso) as retraso_max,
            ROUND(AVG(speed), 1) as vel_media
        FROM train_snapshots
        WHERE cod_comercial IS NOT NULL
        GROUP BY cod_comercial, tipo_tren, des_corridor
        ORDER BY snapshots DESC
        OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY
    """)
    rows = cursor.fetchall()
    if rows:
        print("\n  Trenes capturados (top 20):")
        for cod, tipo, corridor, snaps, avg_ret, max_ret, avg_vel in rows:
            ret_str = f"retraso {avg_ret}min (máx {max_ret}min)" if avg_ret is not None else "sin datos"
            vel_str = f"{avg_vel} km/h" if avg_vel else "sin vel."
            print(f"    [{cod}] {tipo} | {corridor}")
            print(f"           {snaps} snapshots | {ret_str} | {vel_str}")

    cursor.execute("""
        SELECT
            CASE
                WHEN ult_retraso <= 0  THEN 'Puntual'
                WHEN ult_retraso <= 5  THEN '1-5 min'
                WHEN ult_retraso <= 15 THEN '6-15 min'
                WHEN ult_retraso <= 30 THEN '16-30 min'
                ELSE '>30 min'
            END AS categoria,
            COUNT(*) as cnt
        FROM train_snapshots
        WHERE ult_retraso IS NOT NULL
        GROUP BY
            CASE
                WHEN ult_retraso <= 0  THEN 'Puntual'
                WHEN ult_retraso <= 5  THEN '1-5 min'
                WHEN ult_retraso <= 15 THEN '6-15 min'
                WHEN ult_retraso <= 30 THEN '16-30 min'
                ELSE '>30 min'
            END
    """)
    rows = cursor.fetchall()
    if rows:
        total = sum(r[1] for r in rows)
        print("\n  Distribución de retrasos:")
        for cat, cnt in rows:
            print(f"    {cat}: {cnt:,} ({cnt/total*100:.1f}%)")

    cursor.execute("SELECT MIN(captured_at), MAX(captured_at) FROM train_snapshots")
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
        description="Captura largo recorrido Renfe Cádiz↔Madrid → Azure SQL"
    )
    parser.add_argument("--loop", type=int, default=0,
                        help="Intervalo en segundos (0 = captura única)")
    parser.add_argument("--summary", action="store_true")
    parser.add_argument("--init-stations", action="store_true",
                        help="Cargar catálogo de estaciones (ejecutar una vez)")
    parser.add_argument("--flush-every", type=int, default=FLUSH_CYCLES,
                        help="Flush a BD cada N ciclos (default: 10 → 5 min con --loop 30)")
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
            f"Modo loop: captura cada {args.loop}s, "
            f"flush a BD cada {flush_every * args.loop}s "
            f"(Ctrl+C para parar)\n"
        )
        batch = {"snapshots": [], "itineraries": []}
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
        batch = {"snapshots": [], "itineraries": []}
        capture_once(batch)
        conn = get_conn()
        flush_batch(conn, batch)
        conn.close()


if __name__ == "__main__":
    main()
