"""
Captura de datos en tiempo real de Cercanías Cádiz (GTFS-RT Renfe)
===================================================================
Líneas Cercanías Cádiz: C1 (Cádiz - S.Fernando - El Puerto - Jerez - Sevilla)

Uso:
  python renfe_cadiz_cercanias.py                  # Captura única
  python renfe_cadiz_cercanias.py --loop 30        # Captura cada 30s
  python renfe_cadiz_cercanias.py --summary        # Ver resumen
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

CADIZ_BBOX = {
    "lat_min": 36.3,
    "lat_max": 37.5,
    "lon_min": -6.5,
    "lon_max": -5.7,
}

ENDPOINTS = {
    "vehicle_positions": "https://gtfsrt.renfe.com/vehicle_positions.json",
    "trip_updates":      "https://gtfsrt.renfe.com/trip_updates.json",
    "service_alerts":    "https://gtfsrt.renfe.com/alerts.json",
}

REQUEST_TIMEOUT = 15

CADIZ_KEYWORDS = [
    "cádiz", "cadiz", "san fernando", "el puerto", "jerez",
    "sevilla", "chiclana", "puerto real", "barbate",
]

# ╔══════════════════════════════════════════════════════════════╗
# ║  BASE DE DATOS                                               ║
# ╚══════════════════════════════════════════════════════════════╝

def init_db(conn: pyodbc.Connection):
    run_ddl(conn, [
        """
        IF OBJECT_ID('cadiz_vehicle_snapshots', 'U') IS NULL
        CREATE TABLE cadiz_vehicle_snapshots (
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
        IF OBJECT_ID('cadiz_trip_updates', 'U') IS NULL
        CREATE TABLE cadiz_trip_updates (
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
        IF OBJECT_ID('cadiz_service_alerts', 'U') IS NULL
        CREATE TABLE cadiz_service_alerts (
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
                       WHERE name='idx_cdz_vs_captured'
                         AND object_id=OBJECT_ID('cadiz_vehicle_snapshots'))
        CREATE INDEX idx_cdz_vs_captured ON cadiz_vehicle_snapshots(captured_at)
        """,
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes
                       WHERE name='idx_cdz_vs_vehicle_ts'
                         AND object_id=OBJECT_ID('cadiz_vehicle_snapshots'))
        CREATE INDEX idx_cdz_vs_vehicle_ts ON cadiz_vehicle_snapshots(vehicle_id, event_timestamp DESC)
        """,
    ])

    # Eliminar columna raw_json de tablas existentes (si aún existe)
    cursor = conn.cursor()
    for tbl in ["cadiz_vehicle_snapshots", "cadiz_trip_updates", "cadiz_service_alerts"]:
        cursor.execute(
            f"IF COL_LENGTH('{tbl}', 'raw_json') IS NOT NULL "
            f"ALTER TABLE {tbl} DROP COLUMN raw_json"
        )
    conn.commit()
    cursor.close()
    print("Tablas e índices verificados (Azure SQL — Cádiz).")


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
# ║  FETCH Y PROCESAMIENTO                                       ║
# ╚══════════════════════════════════════════════════════════════╝

def fetch_json(url):
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def is_in_cadiz(lat, lon):
    return (CADIZ_BBOX["lat_min"] <= lat <= CADIZ_BBOX["lat_max"]
            and CADIZ_BBOX["lon_min"] <= lon <= CADIZ_BBOX["lon_max"])


def extract_line_from_label(label):
    if not label:
        return None
    return label.split("-")[0] if "-" in label else None


def _get_previous_snapshot(conn, vehicle_id):
    if not vehicle_id:
        return None
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TOP 1 latitude, longitude, event_timestamp, captured_at
        FROM cadiz_vehicle_snapshots
        WHERE vehicle_id = ?
        ORDER BY captured_at DESC
    """, vehicle_id)
    row = cursor.fetchone()
    cursor.close()
    return row


def _get_first_position_captured_at(conn, vehicle_id, lat, lon):
    """
    Devuelve el Unix timestamp de la primera vez que capturamos a este vehículo
    en las coordenadas (lat, lon). Compensa la baja frecuencia de actualización GPS
    de Renfe en cercanías usando el tiempo real de permanencia en cada posición.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MIN(captured_at)
        FROM cadiz_vehicle_snapshots
        WHERE vehicle_id = ?
          AND ABS(latitude  - ?) < 0.0001
          AND ABS(longitude - ?) < 0.0001
    """, vehicle_id, lat, lon)
    row = cursor.fetchone()
    cursor.close()
    if row and row[0]:
        try:
            return datetime.fromisoformat(row[0]).timestamp()
        except Exception:
            return None
    return None


def process_vehicle_positions(conn):
    data = fetch_json(ENDPOINTS["vehicle_positions"])
    feed_ts = data.get("header", {}).get("timestamp")
    now = datetime.now(timezone.utc).isoformat()

    rows = []
    for entity in data.get("entity", []):
        v = entity.get("vehicle", {})
        pos = v.get("position", {})
        lat = pos.get("latitude")
        lon = pos.get("longitude")
        if lat is None or lon is None or not is_in_cadiz(lat, lon):
            continue

        veh = v.get("vehicle", {})
        vehicle_id = veh.get("id")
        event_ts = v.get("timestamp")

        speed, bearing = None, None
        prev = _get_previous_snapshot(conn, vehicle_id)
        if prev:
            prev_lat, prev_lon, _, _prev_captured_at = prev
            pos_changed = (abs(lat - prev_lat) > 0.0001 or abs(lon - prev_lon) > 0.0001)
            if pos_changed:
                ts1 = _get_first_position_captured_at(conn, vehicle_id, prev_lat, prev_lon)
                if ts1 is None:
                    ts1 = datetime.fromisoformat(_prev_captured_at).timestamp()
                ts2 = datetime.fromisoformat(now).timestamp()
                speed, bearing = compute_speed_bearing(prev_lat, prev_lon, ts1, lat, lon, ts2)

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

    if rows:
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO cadiz_vehicle_snapshots
            (captured_at, feed_timestamp, trip_id, vehicle_id, vehicle_label,
             line, latitude, longitude, bearing, speed, current_status,
             stop_id, event_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        cursor.close()
    return len(rows)


def process_trip_updates(conn, known_trip_ids):
    data = fetch_json(ENDPOINTS["trip_updates"])
    feed_ts = data.get("header", {}).get("timestamp")
    now = datetime.now(timezone.utc).isoformat()

    rows = []
    for entity in data.get("entity", []):
        tu = entity.get("tripUpdate", {})
        trip = tu.get("trip", {})
        trip_id = trip.get("tripId")
        if trip_id not in known_trip_ids:
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

    if rows:
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO cadiz_trip_updates
            (captured_at, feed_timestamp, trip_id, line, schedule_relationship,
             stop_sequence, stop_id, arrival_delay, departure_delay)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        cursor.close()
    return len(rows)


def process_service_alerts(conn):
    data = fetch_json(ENDPOINTS["service_alerts"])
    feed_ts = data.get("header", {}).get("timestamp")
    now = datetime.now(timezone.utc).isoformat()

    rows = []
    for entity in data.get("entity", []):
        alert = entity.get("alert", {})
        header_tr = alert.get("headerText", {}).get("translation", [])
        desc_tr = alert.get("descriptionText", {}).get("translation", [])
        header = header_tr[0].get("text", "") if header_tr else ""
        description = desc_tr[0].get("text", "") if desc_tr else ""
        if not any(kw in (header + " " + description).lower() for kw in CADIZ_KEYWORDS):
            continue
        rows.append((now, feed_ts, entity.get("id"),
                     alert.get("cause"), alert.get("effect"),
                     header, description))

    if rows:
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO cadiz_service_alerts
            (captured_at, feed_timestamp, alert_id, cause, effect,
             header_text, description)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        cursor.close()
    return len(rows)


# ╔══════════════════════════════════════════════════════════════╗
# ║  CAPTURA PRINCIPAL                                           ║
# ╚══════════════════════════════════════════════════════════════╝

def capture_once(conn):
    stats = {"positions": 0, "trip_updates": 0, "alerts": 0, "errors": []}
    now = datetime.now()

    try:
        stats["positions"] = process_vehicle_positions(conn)
    except Exception as e:
        stats["errors"].append(f"vehicle_positions: {e}")

    known_trips = set()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT trip_id FROM cadiz_vehicle_snapshots
            WHERE trip_id IS NOT NULL
              AND CAST(captured_at AS DATE) = CAST(GETUTCDATE() AS DATE)
        """)
        known_trips = {r[0] for r in cursor.fetchall()}
        cursor.close()
    except Exception as e:
        stats["errors"].append(f"known_trips: {e}")

    if known_trips:
        try:
            stats["trip_updates"] = process_trip_updates(conn, known_trips)
        except Exception as e:
            stats["errors"].append(f"trip_updates: {e}")

    try:
        stats["alerts"] = process_service_alerts(conn)
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
    print("  RESUMEN — CERCANÍAS CÁDIZ (Azure SQL)")
    print("=" * 60)

    cursor = conn.cursor()
    for table in ["cadiz_vehicle_snapshots", "cadiz_trip_updates", "cadiz_service_alerts"]:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table}: {cursor.fetchone()[0]:,} registros")

    cursor.execute("""
        SELECT line, COUNT(*) as cnt, COUNT(DISTINCT trip_id) as trips
        FROM cadiz_vehicle_snapshots WHERE line IS NOT NULL
        GROUP BY line ORDER BY cnt DESC
    """)
    rows = cursor.fetchall()
    if rows:
        print("\n  Líneas detectadas:")
        for line, cnt, trips in rows:
            print(f"    {line}: {cnt:,} snapshots, {trips} viajes")

    cursor.execute("""
        SELECT line, ROUND(AVG(speed), 1), ROUND(MAX(speed), 1), COUNT(*)
        FROM cadiz_vehicle_snapshots
        WHERE speed IS NOT NULL AND line IS NOT NULL
        GROUP BY line ORDER BY AVG(speed) DESC
    """)
    rows = cursor.fetchall()
    if rows:
        print("\n  Velocidad por línea (km/h):")
        for line, avg_s, max_s, cnt in rows:
            print(f"    {line}: media {avg_s} km/h, máx {max_s} km/h ({cnt} registros)")

    cursor.execute("SELECT MIN(captured_at), MAX(captured_at) FROM cadiz_vehicle_snapshots")
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
        description="Captura Cercanías Cádiz → Azure SQL"
    )
    parser.add_argument("--loop", type=int, default=0)
    parser.add_argument("--summary", action="store_true")
    args = parser.parse_args()

    conn = get_conn()
    init_db(conn)

    if args.summary:
        show_summary(conn)
        conn.close()
        return

    if args.loop > 0:
        print(f"Modo loop: captura cada {args.loop}s (Ctrl+C para parar)\n")
        try:
            while True:
                capture_once(conn)
                time.sleep(args.loop)
        except KeyboardInterrupt:
            print("\nDetenido.")
            show_summary(conn)
    else:
        capture_once(conn)

    conn.close()


if __name__ == "__main__":
    main()
