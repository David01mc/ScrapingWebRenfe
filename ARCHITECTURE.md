# Arquitectura del Sistema — Renfe Scraper en Tiempo Real

## Visión general

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          FUENTES DE DATOS                               │
│                                                                         │
│  ┌─────────────────────────┐    ┌──────────────────────────────────┐   │
│  │   gtfsrt.renfe.com      │    │  tiempo-real.largorecorrido      │   │
│  │                         │    │         .renfe.com               │   │
│  │  /vehicle_positions.json│    │                                  │   │
│  │  /trip_updates.json     │    │  /renfe-visor/flotaLD.json       │   │
│  │  /alerts.json           │    │  /renfe-visor/trenesConEst...    │   │
│  │                         │    │  /data/estaciones.geojson        │   │
│  │  Actualización: ~20s    │    │  Actualización: ~15s             │   │
│  └────────────┬────────────┘    └────────────────┬─────────────────┘   │
└───────────────┼─────────────────────────────────-┼─────────────────────┘
                │  HTTP GET cada 30s               │  HTTP GET cada 30s
                │  (requests)                      │  (requests)
                └──────────────────┬───────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                     AZURE VM — Standard B1s                              │
│                     Ubuntu 22.04 | 68.221.175.21                         │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  systemd: renfe-capture.service                                    │  │
│  │                                                                    │  │
│  │  renfe_capture.py  ──────────────────────────────────────────────  │  │
│  │                                                                    │  │
│  │   ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐   │  │
│  │   │  CERCANÍAS   │  │  CERCANÍAS   │  │   LARGO RECORRIDO    │   │  │
│  │   │   ASTURIAS   │  │    CÁDIZ     │  │   (toda España)      │   │  │
│  │   │              │  │              │  │                       │   │  │
│  │   │ BBox filtro  │  │ BBox filtro  │  │  Flota + Itinerarios │   │  │
│  │   │ lat 43–43.7  │  │ lat 36.3–37.5│  │  ~145 trenes activos │   │  │
│  │   └──────┬───────┘  └──────┬───────┘  └──────────┬──────────┘   │  │
│  │          │                 │                       │              │  │
│  │          └─────────────────┴───────────────────────┘              │  │
│  │                              │                                     │  │
│  │              ┌───────────────▼────────────────┐                   │  │
│  │              │      BATCH EN MEMORIA           │                   │  │
│  │              │                                 │                   │  │
│  │              │  _ast_pos, _cdz_pos, _lr_pos    │                   │  │
│  │              │  Caché velocidad/bearing        │                   │  │
│  │              │                                 │                   │  │
│  │              │  Acumula 480 ciclos × 30s = 4h  │                   │  │
│  │              └───────────────┬────────────────┘                   │  │
│  │                              │  flush cada 4h                      │  │
│  │              ┌───────────────▼────────────────┐                   │  │
│  │              │      azure_db.py               │                   │  │
│  │              │  ODBC Driver 18 for SQL Server  │                   │  │
│  │              │  Connection Timeout = 60s       │                   │  │
│  │              └───────────────┬────────────────┘                   │  │
│  └──────────────────────────────┼───────────────────────────────────┘  │
│                                 │  pyodbc / TCP 1433                    │
│  Logs: /opt/renfe/              │                                        │
│  renfe-capture.log              │                                        │
└─────────────────────────────────┼────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   AZURE SQL DATABASE (Serverless)                        │
│              sqljosedavid.database.windows.net — SQLJoseDavid            │
│              Gen5 · 1 vCore máx · 32 GB · Free tier: 100k vCore-s/mes   │
│                                                                          │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────────────┐  │
│  │    ASTURIAS      │  │      CÁDIZ       │  │   LARGO RECORRIDO     │  │
│  │                  │  │                  │  │                       │  │
│  │ vehicle_snapshots│  │ vehicle_snapshots│  │ train_snapshots       │  │
│  │ trip_updates     │  │ trip_updates     │  │ train_itineraries     │  │
│  │ service_alerts   │  │ service_alerts   │  │ stations              │  │
│  └──────────────────┘  └──────────────────┘  └───────────────────────┘  │
│                                                                          │
│  Auto-pause: tras 1h sin conexiones → 0 vCores consumidos               │
└─────────────────────────────────────────────────────────────────────────┘
                                  ▲
                                  │  SQL (SSMS / pandas / Azure Data Studio)
                                  │
┌─────────────────────────────────┴────────────────────────────────────────┐
│                          PC LOCAL (desarrollo)                            │
│                                                                           │
│   SSMS / Azure Data Studio ──── Consultas y análisis                     │
│   pandas / Jupyter Notebook ─── Análisis de datos y ML                   │
│   VS Code + Claude Code ──────── Desarrollo de scripts                   │
│                                                                           │
│   Subida de scripts:                                                      │
│   scp -i deploy/RenfeKey.pem *.py azureuserRenfe@68.221.175.21:~/        │
└───────────────────────────────────────────────────────────────────────────┘
```

---

## Ciclo de captura

```
t = 0s      Captura #1 — fetch HTTP → procesa → acumula en RAM
t = 30s     Captura #2 — calcula velocidad con caché en memoria
t = 60s     Captura #3
...
t = 14.400s Captura #480 — FLUSH → abre conexión BD → inserta ~12k filas → cierra
t = 14.430s Captura #481 — nuevo ciclo
```

---

## Pausa nocturna

```
        6:00 UTC                           23:00 UTC
          │                                    │
──────────┼────────────────────────────────────┼──────────
          │◄──────── 17h captura activa ───────►│
          │                                    │
          │                              ┌─────┴──────────────┐
          │                              │  PAUSA NOCTURNA    │
          │                              │  7h sin capturas   │
          │                              │  BD en auto-pause  │
          └──────────────────────────────┴────────────────────┘
```

---

## Flujo de datos detallado

```
Renfe GTFS-RT                renfe_capture.py              Azure SQL
─────────────                ────────────────              ──────────

vehicle_positions.json  ──►  Filtra por BBox         ──►  asturias_vehicle_snapshots
                             Calcula speed/bearing         cadiz_vehicle_snapshots
                             (Haversine + caché RAM)

trip_updates.json       ──►  Filtra por trip_id       ──►  asturias_trip_updates
                             conocidos (en memoria)        cadiz_trip_updates

alerts.json             ──►  Filtra por keywords      ──►  asturias_service_alerts
                             (Asturias / Cádiz)            cadiz_service_alerts

flotaLD.json            ──►  Todos los trenes LD      ──►  train_snapshots
                             Calcula speed/bearing

trenesConEstacionesLD   ──►  Solo trenes nuevos       ──►  train_itineraries
                             (1 vez por tren/día)
```

---

## Componentes y tecnologías

| Componente | Tecnología | Detalle |
|---|---|---|
| Captura HTTP | `requests` (Python) | GET cada 30s a endpoints Renfe |
| Parseo | Python dict nativo | JSON → tuplas → batch en RAM |
| Cálculo velocidad | Haversine + caché en memoria | Sin consultas a BD |
| Persistencia | `pyodbc` + ODBC Driver 18 | Flush cada 4h, 1 conexión |
| Base de datos | Azure SQL Serverless | Gen5, 1 vCore máx, 32 GB |
| Ejecución | `systemd` (Ubuntu 22.04) | Auto-restart, arranque con la VM |
| VM | Azure Standard B1s | 1 vCPU, 1 GB RAM, Ubuntu 22.04 |
| Logs | Fichero `/opt/renfe/renfe-capture.log` | Rotación manual |
| Secretos | `.env` + `python-dotenv` | Nunca en repositorio |

---

## Optimización de consumo Azure SQL

```
Versión          Estrategia                        vCore-s/mes    Estado
───────          ──────────                        ───────────    ──────
V1 original      3 scripts, conexión persistente   ~900.000       ✗ Agotado en 2 días
V2 batch 20min   1 script, flush cada 20min        ~30.000        ✗ BD sin auto-pause
V3 batch 2h      1 script, flush 2h + noche        ~80.000        ✗ Cerca del límite
V4 actual        1 script, flush 4h + noche        ~45.000        ✓ PRODUCCIÓN
                 Automatic Tuning desactivado
                 Auto-pause delay = 1h (mín. tier gratuito)
                 1 vCore máx (antes 2 vCores)
                 Largo Recorrido: toda España (sin filtro)

Límite gratuito: 100.000 vCore-s/mes
```

---

*Arquitectura actualizada el 2026-03-17*
