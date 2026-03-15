# Documentación — Proyecto Scraping Renfe en Tiempo Real

## Índice

1. [Resumen del proyecto](#1-resumen-del-proyecto)
2. [Fuentes de datos](#2-fuentes-de-datos)
3. [Los tres scripts de captura](#3-los-tres-scripts-de-captura)
4. [Esquema de Azure SQL Database](#4-esquema-de-azure-sql-database)
5. [Cálculo de velocidad y bearing](#5-cálculo-de-velocidad-y-bearing)
6. [Despliegue en Azure](#6-despliegue-en-azure)
7. [Gestión de los servicios en la VM](#7-gestión-de-los-servicios-en-la-vm)
8. [Consultar y exportar datos](#8-consultar-y-exportar-datos)
9. [Próximos pasos — Análisis y ML](#9-próximos-pasos--análisis-y-ml)

---

## 1. Resumen del proyecto

Proyecto académico para capturar datos de trenes Renfe en tiempo real y almacenarlos en **Azure SQL Database** para análisis posteriores: retrasos, velocidades, puntualidad, predicciones con ML, etc.

Se capturan tres tipos de datos:
- **Posiciones GPS** en tiempo real de los trenes (cada 30 segundos)
- **Retrasos y cancelaciones** por parada y viaje
- **Alertas de servicio** (incidencias, obras, interrupciones)

Todo se ejecuta de forma autónoma en una **VM Azure B1s** (Ubuntu 22.04) sin necesidad de que el ordenador personal esté encendido. Los datos se almacenan centralizadamente en **Azure SQL Database**, accesible desde cualquier cliente SQL (SSMS, Azure Data Studio, DBeaver, pandas...).

### Infraestructura

| Componente | Detalle |
|---|---|
| VM | Azure Standard B1s — Ubuntu 22.04 LTS |
| IP pública VM | *(ver azure_db.py / configuración privada)* |
| Usuario SSH | *(ver configuración privada)* |
| Clave SSH | `deploy/RenfeKey.pem` |
| Scripts en VM | `/opt/renfe/` |
| Base de datos | Azure SQL Database |
| Servidor SQL | *(ver azure_db.py)* |
| Driver | Microsoft ODBC Driver 18 for SQL Server |

---

## 2. Fuentes de datos

### Cercanías — GTFS-RT oficial Renfe (CC BY 4.0)

| Dato | Endpoint | Actualización |
|---|---|---|
| Posiciones GPS | `https://gtfsrt.renfe.com/vehicle_positions.json` | Cada 20 segundos |
| Trip updates (retrasos) | `https://gtfsrt.renfe.com/trip_updates.json` | Cada 20 segundos |
| Alertas | `https://gtfsrt.renfe.com/alerts.json` | Variable |

El JSON de posiciones tiene esta estructura:
```json
{
  "header": { "gtfsRealtimeVersion": "2.0", "timestamp": "1768820619" },
  "entity": [{
    "id": "VP_C1-23533",
    "vehicle": {
      "trip": { "tripId": "3015L23533C1" },
      "position": { "latitude": 43.46, "longitude": -5.83 },
      "currentStatus": "INCOMING_AT",
      "timestamp": "1768820614",
      "stopId": "15301",
      "vehicle": { "id": "23533", "label": "C1-23533-PLATF.(1)" }
    }
  }]
}
```

> **Nota:** Renfe NO publica `speed` ni `bearing` en el GTFS-RT. Se calculan de forma derivada entre snapshots consecutivos (ver sección 5).
>
> **Limitación conocida:** Renfe actualiza las coordenadas GPS de cercanías cada 2-10 minutos (no cada 20s). El `feed_timestamp` del header sí cambia cada 20s, pero las coordenadas permanecen congeladas. Los scripts detectan si la posición realmente cambió antes de calcular velocidad.

### Largo Recorrido — tiempo-real.largorecorrido.renfe.com

| Dato | Endpoint | Actualización |
|---|---|---|
| Posiciones + retrasos | `.../renfe-visor/flotaLD.json` | Cada 15 segundos |
| Itinerarios completos | `.../renfe-visor/trenesConEstacionesLD.json` | Cada 15 segundos |
| Catálogo de estaciones | `.../data/estaciones.geojson` | Estático |

---

## 3. Los tres scripts de captura

### 3.1 `renfe_asturias_cercanias.py` — Cercanías Asturias

Captura datos de los trenes de Cercanías que circulan por Asturias.

**Filtrado:** Bounding box GPS:
```
lat: 43.0 – 43.7
lon: -7.0 – -4.5
```

**Tablas en Azure SQL:** `asturias_vehicle_snapshots`, `asturias_trip_updates`, `asturias_service_alerts`

**Uso:**
```bash
python renfe_asturias_cercanias.py              # Captura única
python renfe_asturias_cercanias.py --loop 30   # Captura cada 30 segundos
python renfe_asturias_cercanias.py --summary   # Ver resumen estadístico
```

---

### 3.2 `renfe_cadiz_cercanias.py` — Cercanías Cádiz

Idéntico en estructura al script de Asturias pero filtrado para la red de Cercanías de Cádiz (línea C1: Cádiz → San Fernando → El Puerto → Jerez → Sevilla).

**Filtrado:** Bounding box GPS:
```
lat: 36.3 – 37.5
lon: -6.5 – -5.7
```

**Tablas en Azure SQL:** `cadiz_vehicle_snapshots`, `cadiz_trip_updates`, `cadiz_service_alerts`

**Uso:**
```bash
python renfe_cadiz_cercanias.py              # Captura única
python renfe_cadiz_cercanias.py --loop 30   # Captura cada 30 segundos
python renfe_cadiz_cercanias.py --summary   # Ver resumen estadístico
```

---

### 3.3 `renfe_largo_recorrido.py` — Largo Recorrido Cádiz ↔ Madrid

Captura datos de los trenes de largo recorrido en el corredor Cádiz ↔ Madrid (AVE, Alvia, Intercity, etc.).

**Filtrado:** Trenes cuyo campo `desCorridor` contiene `"ádiz"` o `"adiz"` (cubre Cádiz en cualquier codificación).

**Tablas en Azure SQL:** `train_snapshots`, `train_itineraries`, `stations`

**Datos capturados por tren:**
- Posición GPS en tiempo real
- Retraso actual en minutos (`ult_retraso`)
- Estación anterior y siguiente (con nombre legible cruzado con `stations`)
- Hora estimada de llegada a la siguiente estación
- Tipo de tren (AVE, Alvia, Intercity, MD...)
- Material rodante
- Itinerario completo con todas las paradas y horarios programados

**Uso:**
```bash
python renfe_largo_recorrido.py --init-stations  # Solo la PRIMERA vez (carga estaciones)
python renfe_largo_recorrido.py                  # Captura única
python renfe_largo_recorrido.py --loop 30        # Captura cada 30 segundos
python renfe_largo_recorrido.py --summary        # Ver resumen estadístico
```

> `--init-stations` descarga el catálogo completo de estaciones Renfe (~1000 estaciones con nombre, coordenadas y provincia) y lo guarda en la tabla `stations`. Solo es necesario ejecutarlo una vez.

---

### 3.4 `azure_db.py` — Módulo de conexión compartido

Módulo importado por los tres scripts. Centraliza la cadena de conexión a Azure SQL y expone dos funciones:

```python
get_conn()              # Devuelve una conexión pyodbc activa
run_ddl(conn, stmts)    # Ejecuta lista de sentencias DDL y hace commit
```

---

## 4. Esquema de Azure SQL Database

Todas las tablas se crean automáticamente al arrancar cada script si no existen (`IF OBJECT_ID(...) IS NULL CREATE TABLE ...`).

### 4.1 Cercanías — `asturias_vehicle_snapshots` / `cadiz_vehicle_snapshots`

Un registro por tren por captura. Mismo esquema con prefijo diferente.

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | INT IDENTITY | Clave primaria autoincremental |
| `captured_at` | NVARCHAR(50) | Timestamp ISO 8601 UTC de la captura |
| `feed_timestamp` | BIGINT | Timestamp del header del feed GTFS-RT |
| `trip_id` | NVARCHAR(100) | ID del viaje, ej: `"3015L23533C1"` |
| `vehicle_id` | NVARCHAR(50) | ID del vehículo, ej: `"23533"` |
| `vehicle_label` | NVARCHAR(200) | Label completo, ej: `"C1-23533-PLATF.(1)"` |
| `line` | NVARCHAR(20) | Línea extraída del label, ej: `"C1"` |
| `latitude` | FLOAT | Latitud GPS |
| `longitude` | FLOAT | Longitud GPS |
| `bearing` | FLOAT | Rumbo calculado en grados 0-360 (NULL si sin movimiento) |
| `speed` | FLOAT | Velocidad calculada en km/h (NULL si sin movimiento) |
| `current_status` | NVARCHAR(50) | `INCOMING_AT`, `STOPPED_AT`, `IN_TRANSIT_TO` |
| `stop_id` | NVARCHAR(50) | Código de la parada actual |
| `event_timestamp` | BIGINT | Timestamp GPS del vehículo individual |

### 4.2 Cercanías — `asturias_trip_updates` / `cadiz_trip_updates`

Retrasos por parada y viaje.

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | INT IDENTITY | Clave primaria |
| `captured_at` | NVARCHAR(50) | Timestamp de captura |
| `feed_timestamp` | BIGINT | Timestamp del feed |
| `trip_id` | NVARCHAR(100) | ID del viaje |
| `line` | NVARCHAR(20) | Línea |
| `schedule_relationship` | NVARCHAR(50) | Estado del viaje |
| `stop_sequence` | INT | Orden de la parada |
| `stop_id` | NVARCHAR(50) | Código de la parada |
| `arrival_delay` | INT | Retraso de llegada en segundos |
| `departure_delay` | INT | Retraso de salida en segundos |

### 4.3 Cercanías — `asturias_service_alerts` / `cadiz_service_alerts`

Alertas e incidencias filtradas por keywords de la zona (nombres de ciudades y estaciones).

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | INT IDENTITY | Clave primaria |
| `captured_at` | NVARCHAR(50) | Timestamp de captura |
| `feed_timestamp` | BIGINT | Timestamp del feed |
| `alert_id` | NVARCHAR(100) | ID de la alerta |
| `cause` | NVARCHAR(100) | Causa (TECHNICAL_PROBLEM, STRIKE, etc.) |
| `effect` | NVARCHAR(100) | Efecto (REDUCED_SERVICE, NO_SERVICE, etc.) |
| `header_text` | NVARCHAR(MAX) | Título de la alerta |
| `description` | NVARCHAR(MAX) | Descripción completa |

### 4.4 Largo Recorrido — `train_snapshots`

Un registro por tren por captura.

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | INT IDENTITY | Clave primaria |
| `captured_at` | NVARCHAR(50) | Timestamp ISO 8601 UTC |
| `cod_comercial` | NVARCHAR(20) | Número de tren, ej: `"01601"` |
| `cod_product` | INT | Código de tipo de tren |
| `tipo_tren` | NVARCHAR(50) | Descripción del tipo (AVE, Alvia, etc.) |
| `des_corridor` | NVARCHAR(200) | Corredor, ej: `"Madrid PA - Cádiz"` |
| `cod_origen` | NVARCHAR(20) | Código estación origen |
| `cod_destino` | NVARCHAR(20) | Código estación destino |
| `latitude` | FLOAT | Latitud GPS actual |
| `longitude` | FLOAT | Longitud GPS actual |
| `speed` | FLOAT | Velocidad calculada en km/h |
| `bearing` | FLOAT | Rumbo calculado en grados 0-360 |
| `ult_retraso` | INT | Retraso actual en minutos |
| `cod_est_ant` | NVARCHAR(20) | Código estación anterior |
| `cod_est_sig` | NVARCHAR(20) | Código siguiente estación |
| `hora_llegada_sig_est` | NVARCHAR(10) | Hora estimada llegada a siguiente estación |
| `plataforma` | NVARCHAR(10) | Vía/andén |
| `material` | NVARCHAR(50) | Código de composición del tren |
| `accesible` | INT | 1 = accesible PMR |
| `gps_timestamp` | BIGINT | Timestamp Unix del GPS |

### 4.5 Largo Recorrido — `train_itineraries`

Itinerario completo de cada tren (todas las paradas con hora programada).

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | INT IDENTITY | Clave primaria |
| `captured_at` | NVARCHAR(50) | Timestamp de captura |
| `cod_comercial` | NVARCHAR(20) | Número de tren |
| `stop_order` | INT | Orden de la parada (0-based) |
| `station_code` | NVARCHAR(20) | Código de la estación |
| `hora_prog` | NVARCHAR(10) | Hora programada `"HH:MM"` |
| `latitude` | FLOAT | Latitud GPS de la parada |
| `longitude` | FLOAT | Longitud GPS de la parada |

### 4.6 Largo Recorrido — `stations`

Catálogo completo de estaciones Renfe (~1000 estaciones). Se carga una vez con `--init-stations`.

| Columna | Tipo | Descripción |
|---|---|---|
| `codigo` | INT | Código único de la estación (PK) |
| `nombre` | NVARCHAR(200) | Nombre de la estación |
| `latitude` | FLOAT | Latitud |
| `longitude` | FLOAT | Longitud |
| `localidad` | NVARCHAR(200) | Localidad |
| `provincia` | NVARCHAR(100) | Provincia |
| `es_cercanias` | INT | 1 si tiene servicio de cercanías |

---

## 5. Cálculo de velocidad y bearing

Renfe no publica velocidad ni rumbo en sus feeds. Se calculan comparando snapshots consecutivos del mismo vehículo.

### Algoritmo — compensación de baja frecuencia GPS

El GPS de cercanías se actualiza cada **2-10 minutos** (no cada 20s como el feed). Para evitar velocidades infladas:

1. Se consulta el snapshot anterior del mismo `vehicle_id`.
2. Si la posición **no ha cambiado** (`ABS(lat_nueva - lat_vieja) < 0.0001`): `speed = NULL`. El tren no se ha movido según los datos disponibles.
3. Si la posición **ha cambiado**: se busca la **primera vez** que se capturó la posición anterior (`MIN(captured_at)` con esas coordenadas). Esto da el tiempo real que el tren llevaba en ese punto.
4. `velocidad = distancia_Haversine / tiempo_real_transcurrido`
5. Se filtran valores ruidosos:
   - `< 0.5 km/h` → `NULL` (tren parado con drift GPS)
   - `> 200 km/h` para cercanías → `NULL`
   - `> 350 km/h` para largo recorrido → `NULL`

### Fórmula de Haversine (distancia entre dos puntos GPS)

```python
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371  # radio Tierra en km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
```

### Bearing (rumbo 0-360°)

Calculado con la fórmula de rumbo esférico entre los dos puntos GPS donde se detectó movimiento real.

---

## 6. Despliegue en Azure

### 6.1 Recursos creados en Azure

| Recurso | Detalle |
|---|---|
| Máquina virtual | Standard B1s — Ubuntu 22.04 |
| Azure SQL Database | Ver `azure_db.py` para servidor y base de datos |
| Regla de firewall SQL | IP de la VM añadida en Azure Portal → SQL Server → Networking |

### 6.2 Preparar la clave SSH en tu PC (PowerShell)

```powershell
# Dar permisos correctos a la clave (SSH la rechaza si tiene permisos amplios)
icacls "deploy\RenfeKey.pem" /inheritance:r
icacls "deploy\RenfeKey.pem" /grant:r "${env:USERNAME}:(R)"
```

### 6.3 Subir los scripts a la VM

Desde el directorio del proyecto (`C:\Users\Usuario\Desktop\Scripts\App\ScrapingWebRenfe`):

```powershell
scp -i deploy\RenfeKey.pem `
    renfe_asturias_cercanias.py renfe_cadiz_cercanias.py renfe_largo_recorrido.py azure_db.py `
    <usuario>@<ip-vm>:~/

scp -i deploy\RenfeKey.pem deploy\setup.sh <usuario>@<ip-vm>:~/
```

### 6.4 Conectarse a la VM

```powershell
ssh -i deploy\RenfeKey.pem <usuario>@<ip-vm>
```

### 6.5 Ejecutar el setup en la VM (solo la primera vez)

Una vez conectado por SSH:

```bash
chmod +x ~/setup.sh
sudo ~/setup.sh
```

El script `setup.sh` hace automáticamente:
1. Actualiza el sistema (`apt-get update && upgrade`)
2. Instala Python 3 y pip
3. Instala **Microsoft ODBC Driver 18 for SQL Server**
4. Instala `requests` y `pyodbc` vía pip
5. Copia los scripts a `/opt/renfe/`
6. Crea e inicia los 3 servicios systemd con `--loop 30`

```bash
# Tras el setup, cargar catálogo de estaciones (largo recorrido, solo una vez):
python3 /opt/renfe/renfe_largo_recorrido.py --init-stations
```

### 6.6 Actualizar scripts en la VM

Cuando se modifica un script localmente:

```powershell
# Subir desde Windows
scp -i deploy\RenfeKey.pem renfe_asturias_cercanias.py <usuario>@<ip-vm>:~/

# En la VM: copiar y reiniciar
sudo cp ~/renfe_asturias_cercanias.py /opt/renfe/
sudo systemctl restart renfe-asturias
```

---

## 7. Gestión de los servicios en la VM

Los 3 scripts corren como servicios systemd y arrancan automáticamente si la VM se reinicia.

| Servicio | Script | Log |
|---|---|---|
| `renfe-asturias` | `renfe_asturias_cercanias.py --loop 30` | `/var/log/renfe-asturias.log` |
| `renfe-cadiz` | `renfe_cadiz_cercanias.py --loop 30` | `/var/log/renfe-cadiz.log` |
| `renfe-largo` | `renfe_largo_recorrido.py --loop 30` | `/var/log/renfe-largo.log` |

### Comandos útiles

```bash
# Estado de todos los servicios
sudo systemctl status renfe-asturias renfe-cadiz renfe-largo

# Ver logs en tiempo real
journalctl -u renfe-asturias -f
journalctl -u renfe-cadiz -f
journalctl -u renfe-largo -f

# Ver los últimos 50 registros
journalctl -u renfe-asturias -n 50

# Reiniciar / Parar / Arrancar
sudo systemctl restart renfe-asturias
sudo systemctl stop renfe-cadiz
sudo systemctl start renfe-largo
```

---

## 8. Consultar y exportar datos

Los datos están en Azure SQL Database, accesible desde cualquier cliente SQL:

- **SSMS / Azure Data Studio**: conectar al servidor y base de datos configurados en `azure_db.py`
- **DBeaver**: mismo servidor con driver SQL Server
- **Python/pandas**: ver más abajo

> Asegúrate de que tu IP local está en las reglas de firewall de Azure SQL (Azure Portal → SQL Server → Networking → Firewall rules).

### Consultas de ejemplo (T-SQL)

```sql
-- Snapshots recientes de Asturias
SELECT TOP 100 *
FROM asturias_vehicle_snapshots
ORDER BY captured_at DESC;

-- Velocidad media por línea en Cádiz
SELECT line,
       ROUND(AVG(speed), 1) AS avg_speed_kmh,
       ROUND(MAX(speed), 1) AS max_speed_kmh,
       COUNT(*)             AS registros
FROM cadiz_vehicle_snapshots
WHERE speed IS NOT NULL AND line IS NOT NULL
GROUP BY line
ORDER BY avg_speed_kmh DESC;

-- Retrasos últimas 24h en Asturias
SELECT line,
       ROUND(AVG(CAST(arrival_delay AS FLOAT)) / 60.0, 1) AS avg_delay_min,
       MAX(arrival_delay) / 60                             AS max_delay_min,
       COUNT(*)                                            AS registros
FROM asturias_trip_updates
WHERE captured_at >= CONVERT(NVARCHAR, DATEADD(hour, -24, GETUTCDATE()), 126)
  AND arrival_delay IS NOT NULL
GROUP BY line;

-- Trenes de largo recorrido en ruta ahora mismo
SELECT cod_comercial, tipo_tren, des_corridor,
       latitude, longitude, speed, ult_retraso,
       hora_llegada_sig_est
FROM train_snapshots
WHERE captured_at = (SELECT MAX(captured_at) FROM train_snapshots);
```

### Exportar a pandas desde Python

```python
import pyodbc
import pandas as pd

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=<servidor>.database.windows.net,1433;"
    "DATABASE=<base_de_datos>;"
    "UID=<usuario_sql>;"
    "PWD=<password>;"
    "Encrypt=yes;TrustServerCertificate=no;"
)
conn = pyodbc.connect(conn_str)

df_asturias = pd.read_sql(
    "SELECT * FROM asturias_vehicle_snapshots WHERE speed IS NOT NULL", conn
)
df_cadiz = pd.read_sql(
    "SELECT * FROM cadiz_vehicle_snapshots WHERE speed IS NOT NULL", conn
)
df_largo = pd.read_sql(
    "SELECT * FROM train_snapshots", conn
)
conn.close()
```

---

## 9. Próximos pasos — Análisis y ML

### Resumen disponible en cualquier momento

```bash
python renfe_asturias_cercanias.py --summary      # Asturias
python renfe_cadiz_cercanias.py --summary    # Cádiz
python renfe_largo_recorrido.py --summary    # Largo recorrido
```

Muestra: snapshots por línea, velocidad media/máxima, retrasos medios/máximos, rango temporal.

### Ideas para análisis y modelos ML

| Análisis | Datos necesarios | Tablas |
|---|---|---|
| Predicción de retrasos | `arrival_delay` + hora + día + línea | `*_trip_updates` |
| Velocidad media por tramo | `speed` + `stop_id` consecutivos | `*_vehicle_snapshots` |
| Puntualidad histórica por línea | `arrival_delay` agrupado por `line` + fecha | `*_trip_updates` |
| Predicción ETA largo recorrido | `ult_retraso` + `hora_llegada_sig_est` | `train_snapshots` |
| Anomalías de velocidad | `speed` fuera de rango normal por tramo | Ambos |
| Patrones de retraso por hora | `arrival_delay` + hora de `captured_at` | `*_trip_updates` |
| Reconstrucción de trayecto | `latitude`, `longitude`, `bearing` ordenados por `captured_at` | `*_vehicle_snapshots` |

---

*Documentación actualizada el 2026-03-15*
