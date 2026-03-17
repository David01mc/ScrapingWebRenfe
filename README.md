# Documentación — Proyecto Scraping Renfe en Tiempo Real

## Índice

1. [Resumen del proyecto](#1-resumen-del-proyecto)
2. [Optimización de vCore-segundos](#2-optimización-de-vcore-segundos)
3. [Fuentes de datos](#3-fuentes-de-datos)
4. [Scripts](#4-scripts)
5. [Esquema de Azure SQL Database](#5-esquema-de-azure-sql-database)
6. [Cálculo de velocidad y bearing](#6-cálculo-de-velocidad-y-bearing)
7. [Puesta en marcha — paso a paso](#7-puesta-en-marcha--paso-a-paso)
8. [Despliegue en Azure](#8-despliegue-en-azure)
9. [Gestión del servicio en la VM](#9-gestión-del-servicio-en-la-vm)
10. [Consultar y exportar datos](#10-consultar-y-exportar-datos)
11. [Próximos pasos — Análisis y ML](#11-próximos-pasos--análisis-y-ml)

---

## 1. Resumen del proyecto

Proyecto académico para capturar datos de trenes Renfe en tiempo real y almacenarlos en **Azure SQL Database** para análisis posteriores: retrasos, velocidades, puntualidad, predicciones con ML, etc.

Se capturan tres tipos de datos:
- **Posiciones GPS** en tiempo real de los trenes (cada 30 segundos)
- **Retrasos y cancelaciones** por parada y viaje
- **Alertas de servicio** (incidencias, obras, interrupciones)

Todo se ejecuta de forma autónoma en una **VM Azure B1s** (Ubuntu 22.04) sin necesidad de que el ordenador personal esté encendido. Los datos se almacenan centralizadamente en **Azure SQL Database**, accesible desde cualquier cliente SQL (SSMS, Azure Data Studio, DBeaver, pandas...).

### Sistema de captura optimizado

Para reducir al máximo el consumo de vCore-segundos del tier gratuito de Azure SQL:

| Optimización | Detalle |
|---|---|
| **Script unificado** | Un solo proceso (`renfe_capture.py`) gestiona las 3 fuentes — 1 conexión por flush en lugar de 3 |
| **Flush cada 4 horas** | 480 ciclos × 30s — la BD puede auto-pausarse entre flushes (auto-pause delay mínimo: 1 hora) |
| **Pausa nocturna** | Sin capturas entre las 23:00 y las 06:00 UTC — trenes sin servicio de madrugada |
| **Itinerarios sin duplicados** | El itinerario de cada tren se guarda una sola vez por día (~90% menos filas en `train_itineraries`) |
| **Caché en memoria** | Velocidad y bearing calculados sin consultar la BD — los cachés viven en RAM |

### Infraestructura

| Componente | Detalle |
|---|---|
| VM | Azure Standard B1s — Ubuntu 22.04 LTS |
| IP pública VM | `68.221.175.21` |
| Usuario SSH | `azureuserRenfe` |
| Clave SSH | `deploy/RenfeKey.pem` |
| Scripts en VM | `/opt/renfe/` |
| Base de datos | Azure SQL Database — `SQLJoseDavid` |
| Servidor SQL | `sqljosedavid.database.windows.net` |
| Driver | Microsoft ODBC Driver 18 for SQL Server |

---

---

## 2. Optimización de vCore-segundos

### ¿Qué son los vCore-segundos?

Azure SQL Database Serverless (tier gratuito) no cobra por tiempo de reloj sino por **vCore-segundos**: el producto de los vCores usados por el tiempo que la BD está activa. El tier gratuito incluye **100.000 vCore-segundos al mes**.

Cuando la BD lleva más de **1 hora sin recibir conexiones**, entra en **auto-pause** y el consumo cae a 0. En cuanto llega una nueva conexión, se reactiva (~30-60 segundos de arranque) y vuelve a consumir.

```
vCore-segundos/s = vCores activos × tiempo activo (s)
Mínimo activo    = 0,5 vCores (cuando está "caliente" pero sin carga)
Auto-pause       = 0 vCores   (tras >1 hora sin conexiones)
```

### El problema: conexiones frecuentes impiden el auto-pause

Si el script conecta a la BD cada pocos minutos, el timer de auto-pause se resetea continuamente. La BD permanece "caliente" consumiendo 0,5 vCores de forma ininterrumpida:

```
0,5 vCores × 3.600 s/hora × 17 horas activas/día = 30.600 vCore-s/día
30.600 × 30 días = ~918.000 vCore-s/mes   ← 9× el límite gratuito
```

### Evolución del consumo — versiones del proyecto

```
 vCore-s restantes (límite: 100.000/mes)
 │
100k ┤████████████████████████████████████████████████  Inicio de mes
     │
 80k ┤╲  V1 — conexión persistente (3 scripts, siempre conectados)
     │  ╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲
 60k ┤                  ╲╲╲╲╲╲╲╲╲╲╲
     │                             ╲╲╲╲╲╲╲
 40k ┤                                    ╲╲╲╲╲╲╲
     │                                           ╲ Se agota en ~2 días
 20k ┤                                            ╲╲╲╲
     │                                                ╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲
  0k ┤                                                             ← día 2
     │
     │  V2 — batch 20 min (mejor, pero BD nunca auto-pausa)
 80k ┤╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲
     │                                                            ~30k/mes
     │
     │  V3 — batch 2 horas + pausa nocturna
 80k ┤╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲╲
     │                                                            ~80k/mes
     │
     │  V4 — batch 4 horas + pausa nocturna (actual)             ← ACTUAL
 80k ┤────────────────────────────────────────────────────────────────────
     │                                                             ~45k/mes
  0k └──────────────────────────────────────────────────────────────────→
     día 1                    día 15                           día 30
```

### ¿Por qué el flush cada 4 horas?

Azure SQL tiene un **auto-pause delay mínimo configurable de 1 hora**. Con flush cada 4 horas:

1. A las 0 min → flush (BD activa ~60 s, incluyendo reanudación)
2. De 1 min a ~60 min → BD se enfría y entra en auto-pause (0 vCore-s)
3. De 60 min a 240 min → BD sigue pausada (~3 horas sin consumo)
4. A los 240 min → siguiente flush (vuelve a activarse ~60 s)

```
Consumo real por flush:
  ~750 vCore-s (activación + insert + período de enfriamiento)

Al día (17 h activas, flush cada 4 h):
  ~4 flushes × 750 vCore-s = ~3.000 vCore-s/día

Al mes:
  3.000 × 30 = ~90.000 vCore-s/mes  →  90% del límite gratuito
```

> Con 2 horas de flush el consumo era ~80.000 vCore-s/mes (datos reales medidos). Pasando a 4 horas se reduce a ~45.000 vCore-s/mes al duplicar el tiempo de auto-pause entre conexiones.

### Resumen comparativo

| Versión | Estrategia | vCore-s/mes estimados | Límite agotado en |
|---|---|---|---|
| V1 original | 3 scripts, conexión persistente | ~918.000 | < 2 días |
| V2 batch 20 min | 1 script, flush cada 20 min | ~30.000 | ~3 meses |
| V3 batch 2 horas | 1 script, flush cada 2 h + pausa nocturna | ~80.000 | ~1 mes |
| **V4 actual** | **1 script, flush cada 4 h + pausa nocturna** | **~45.000** | **~2 meses** |
| Límite gratuito | — | 100.000 | — |

### ¿Las velocidades se ven afectadas?

No. El cálculo de velocidad y bearing se realiza en **memoria RAM** cada 30 segundos, independientemente del flush. La BD solo recibe datos ya calculados en el momento del insert.

---

## 3. Fuentes de datos

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
> **Limitación conocida:** Renfe actualiza las coordenadas GPS de cercanías cada 2-10 minutos (no cada 20s como el feed). El `feed_timestamp` del header sí cambia cada 20s, pero las coordenadas permanecen congeladas. Los scripts detectan si la posición realmente cambió antes de calcular velocidad.

### Largo Recorrido — tiempo-real.largorecorrido.renfe.com

| Dato | Endpoint | Actualización |
|---|---|---|
| Posiciones + retrasos | `.../renfe-visor/flotaLD.json` | Cada 15 segundos |
| Itinerarios completos | `.../renfe-visor/trenesConEstacionesLD.json` | Cada 15 segundos |
| Catálogo de estaciones | `.../data/estaciones.geojson` | Estático |

---

## 3. Scripts

### 3.1 `renfe_capture.py` — Script unificado (producción)

Captura las tres fuentes en un único proceso: Cercanías Asturias, Cercanías Cádiz y Largo Recorrido Cádiz ↔ Madrid.

**Uso:**
```bash
python renfe_capture.py                          # Captura única
python renfe_capture.py --loop 30               # Loop: captura 30s, flush 2 h, pausa nocturna
python renfe_capture.py --summary               # Resumen de todas las tablas
python renfe_capture.py --init-stations         # Cargar catálogo de estaciones (solo 1ª vez)
python renfe_capture.py --loop 30 --flush-every 20   # Flush cada 10 min
```

**Fuentes de datos:**

| Fuente | Filtrado | Tablas |
|---|---|---|
| Cercanías Asturias | Bounding box `lat 43.0–43.7 / lon -7.0–-4.5` | `asturias_vehicle_snapshots`, `asturias_trip_updates`, `asturias_service_alerts` |
| Cercanías Cádiz | Bounding box `lat 36.3–37.5 / lon -6.5–-5.7` | `cadiz_vehicle_snapshots`, `cadiz_trip_updates`, `cadiz_service_alerts` |
| Largo Recorrido | `desCorridor` contiene `"ádiz"` o `"adiz"` | `train_snapshots`, `train_itineraries`, `stations` |

---

### 3.2 Scripts individuales (desarrollo / depuración)

Los scripts individuales `renfe_asturias_cercanias.py`, `renfe_cadiz_cercanias.py` y `renfe_largo_recorrido.py` se mantienen para pruebas locales pero **no se usan en producción**. El servicio systemd en la VM ejecuta únicamente `renfe_capture.py`.

---

### 3.3 `azure_db.py` — Módulo de conexión compartido

Lee las credenciales del fichero `.env` y centraliza la conexión a Azure SQL.

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

Alertas e incidencias filtradas por keywords de la zona.

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

### Caché en memoria

Desde la refactorización al sistema de batch, el cálculo de velocidad ya **no consulta la BD**. En su lugar se usan dos cachés en memoria:

- `_position_cache`: última posición conocida por `vehicle_id`
- `_first_pos_cache`: primera vez que se vio cada vehículo en cada posición (para compensar GPS lento)

Ambos cachés se mantienen durante toda la sesión del proceso. Si el servicio se reinicia, los primeros ciclos no tendrán velocidad calculada hasta acumular al menos dos posiciones en memoria.

### Algoritmo — compensación de baja frecuencia GPS

El GPS de cercanías se actualiza cada **2-10 minutos** (no cada 20s como el feed). Para evitar velocidades infladas:

1. Se consulta el snapshot anterior del mismo `vehicle_id` en `_position_cache`.
2. Si la posición **no ha cambiado** (`ABS(lat_nueva - lat_vieja) < 0.0001`): `speed = NULL`.
3. Si la posición **ha cambiado**: se busca en `_first_pos_cache` la primera vez que se capturó la posición anterior. Esto da el tiempo real que el tren llevaba en ese punto.
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

---

## 6. Puesta en marcha — paso a paso

Esta sección cubre todo el proceso desde cero: configurar credenciales, subir archivos a la VM y arrancar los servicios.

### Prerrequisitos

- VM Azure B1s en marcha con Ubuntu 22.04
- Azure SQL Database creada y accesible desde la IP de la VM
- Clave SSH `deploy/RenfeKey.pem` disponible localmente

---

### Paso 1 — Configurar el archivo `.env`

Crea o edita el fichero `.env` en la raíz del proyecto con las credenciales de Azure SQL:

```env
DB_SERVER=sqljosedavid.database.windows.net
DB_NAME=SQLJoseDavid
DB_USER=JoseDavid
DB_PASSWORD=tu_password_aqui
```

> El `.env` está en `.gitignore` y **nunca se sube al repositorio**.

---

### Paso 2 — Preparar los permisos de la clave SSH (solo Windows, solo la primera vez)

Ejecuta en PowerShell desde la raíz del proyecto:

```powershell
icacls "deploy\RenfeKey.pem" /inheritance:r
icacls "deploy\RenfeKey.pem" /grant:r "${env:USERNAME}:(R)"
```

---

### Paso 3 — Subir todos los archivos a la VM

Desde la raíz del proyecto en tu terminal local:

```bash
scp -i deploy/RenfeKey.pem \
    renfe_capture.py \
    azure_db.py \
    .env \
    deploy/setup.sh \
    azureuserRenfe@68.221.175.21:/home/azureuserRenfe/
```

---

### Paso 4 — Conectarse a la VM

```bash
ssh -i deploy/RenfeKey.pem azureuserRenfe@68.221.175.21
```

---

### Paso 5 — Ejecutar el setup (solo la primera vez)

Una vez conectado por SSH:

```bash
chmod +x ~/setup.sh
sudo bash ~/setup.sh
```

El script `setup.sh` hace automáticamente:
1. Actualiza el sistema (`apt-get update && upgrade`)
2. Instala Python 3 y pip
3. Instala Microsoft ODBC Driver 18 for SQL Server
4. Instala `requests`, `pyodbc` y `python-dotenv`
5. Copia los scripts a `/opt/renfe/`
6. Crea e inicia los 3 servicios systemd con `--loop 30`

---

### Paso 6 — Copiar el `.env` a `/opt/renfe/`

```bash
sudo cp /home/azureuserRenfe/.env /opt/renfe/.env
sudo chmod 644 /opt/renfe/.env
```

> **Importante:** el `.env` debe tener permisos `644` (no `600`), ya que los scripts se ejecutan con distintos usuarios según el contexto (systemd vs. terminal).

---

### Paso 7 — Cargar el catálogo de estaciones (solo la primera vez)

Solo necesario para el script de largo recorrido:

```bash
python3 /opt/renfe/renfe_largo_recorrido.py --init-stations
```

---

### Paso 8 — Reiniciar y verificar los servicios

```bash
sudo systemctl restart renfe-asturias renfe-cadiz renfe-largo
sudo systemctl status renfe-asturias renfe-cadiz renfe-largo
```

Los tres servicios deben aparecer como `active (running)`.

---

### Paso 9 — Comprobar los logs

```bash
journalctl -u renfe-capture -f
```

Deberías ver algo así:

```
Tablas e índices verificados (Azure SQL — Unificado).
Captura cada 30s | Flush cada 7200s | Pausa nocturna 23:00–06:00 UTC

[2026-03-15 21:30:00] AST 8pos/12upd  CDZ 5pos/8upd  LR 3snap/0itin  2.1s
[2026-03-15 21:30:30] AST 8pos/11upd  CDZ 5pos/7upd  LR 3snap/0itin  1.9s
...
  → Flush BD | AST: 1600pos/2400upd | CDZ: 1000pos/1600upd | LR: 600snap/180itin | total 7380 filas
```

El flush aparece cada 4 horas. Los itinerarios (`itin`) solo aparecen la primera vez que se ve cada tren en el día.

---

### Descargar el log desde la VM a tu PC

El script `deploy/sync_dbs.sh` descarga el log de captura desde la VM y muestra un resumen:

```bash
bash deploy/sync_dbs.sh
```

El log se guarda en `logs/renfe-capture_YYYYMMDD_HHMMSS.log` con un resumen automático:

```
Descargando log desde 68.221.175.21...
Log guardado en: logs/renfe-capture_20260316_210500.log

=== Resumen ===
Total líneas    : 4320
Flushes a BD    : 8
Warnings/Errors : 0
Primer registro : 2026-03-16 06:00:05 INFO CAPTURA | ...
Último registro : 2026-03-16 22:41:05 INFO FLUSH | ...
```

---

### Actualizar scripts en el futuro

```bash
# Desde tu PC
scp -i deploy/RenfeKey.pem renfe_capture.py azure_db.py azureuserRenfe@68.221.175.21:/home/azureuserRenfe/

# En la VM
sudo cp /home/azureuserRenfe/renfe_capture.py /opt/renfe/
sudo cp /home/azureuserRenfe/azure_db.py /opt/renfe/
sudo systemctl restart renfe-capture
```

---

## 7. Despliegue en Azure

### 7.1 Recursos creados en Azure

| Recurso | Detalle |
|---|---|
| Máquina virtual | Standard B1s — Ubuntu 22.04 |
| Azure SQL Database | `SQLJoseDavid` en `sqljosedavid.database.windows.net` |
| Regla de firewall SQL | IP de la VM añadida en Azure Portal → SQL Server → Networking |

### 7.2 Preparar la clave SSH en Windows (PowerShell)

```powershell
icacls "deploy\RenfeKey.pem" /inheritance:r
icacls "deploy\RenfeKey.pem" /grant:r "${env:USERNAME}:(R)"
```

---

## 8. Gestión del servicio en la VM

Un único servicio systemd gestiona las tres fuentes y arranca automáticamente si la VM se reinicia.

| Servicio | Script | Captura | Flush | Pausa nocturna |
|---|---|---|---|---|
| `renfe-capture` | `renfe_capture.py --loop 30 --flush-every 480` | 30s | 4 horas | 23:00–06:00 UTC |

### Comandos útiles

```bash
# Estado del servicio
sudo systemctl status renfe-capture

# Reiniciar / Parar / Arrancar
sudo systemctl restart renfe-capture
sudo systemctl stop renfe-capture
sudo systemctl start renfe-capture

# Ejecutar manualmente para depurar (muestra el traceback directamente)
python3 /opt/renfe/renfe_capture.py

# Ver resumen de datos capturados
python3 /opt/renfe/renfe_capture.py --summary
```

### Logs

El script escribe un log detallado en `/opt/renfe/renfe-capture.log` con una línea por ciclo de captura y una línea especial por cada flush a BD.

```bash
# Ver logs en tiempo real
tail -f /opt/renfe/renfe-capture.log

# Ver solo los flushes a BD
grep "FLUSH" /opt/renfe/renfe-capture.log

# Ver solo errores y avisos
grep -E "ERROR|WARNING" /opt/renfe/renfe-capture.log

# Ver los últimos 50 registros
tail -50 /opt/renfe/renfe-capture.log
```

**Formato del log:**

```
2026-03-16 21:02:15 INFO CAPTURA | AST 26pos/1upd  CDZ 5pos/4upd  LR 145snap/0itin  0.8s
2026-03-16 21:02:45 INFO CAPTURA | AST 24pos/0upd  CDZ 4pos/3upd  LR 143snap/0itin  0.7s
...
2026-03-17 01:02:15 INFO FLUSH | AST: 1820pos/70upd | CDZ: 350pos/280upd | LR: 10150snap/0itin | total 12670 filas
```

- **CAPTURA**: línea cada 30 segundos con el número de registros acumulados en el ciclo actual.
- **FLUSH**: línea cada 4 horas con el total de filas insertadas en Azure SQL.
- **WARNING**: captura con errores parciales (uno de los feeds falló, el resto continúa).
- **ERROR**: error crítico que ha provocado el cierre del proceso.

---

## 9. Consultar y exportar datos

Los datos están en Azure SQL Database, accesible desde cualquier cliente SQL:

- **SSMS / Azure Data Studio**: conectar al servidor `sqljosedavid.database.windows.net`
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
from dotenv import load_dotenv
import os

load_dotenv(".env")

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={os.environ['DB_SERVER']},1433;"
    f"DATABASE={os.environ['DB_NAME']};"
    f"UID={os.environ['DB_USER']};"
    f"PWD={os.environ['DB_PASSWORD']};"
    "Encrypt=yes;TrustServerCertificate=no;"
)
conn = pyodbc.connect(conn_str)

df_asturias = pd.read_sql("SELECT * FROM asturias_vehicle_snapshots WHERE speed IS NOT NULL", conn)
df_cadiz    = pd.read_sql("SELECT * FROM cadiz_vehicle_snapshots WHERE speed IS NOT NULL", conn)
df_largo    = pd.read_sql("SELECT * FROM train_snapshots", conn)
conn.close()
```

---

## 10. Próximos pasos — Análisis y ML

### Resumen disponible en cualquier momento

```bash
python renfe_asturias_cercanias.py --summary
python renfe_cadiz_cercanias.py --summary
python renfe_largo_recorrido.py --summary
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

*Documentación actualizada el 2026-03-17*
