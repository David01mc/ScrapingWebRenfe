#!/bin/bash
# =============================================================================
# setup.sh — Configuración inicial de la VM Azure (Ubuntu 22.04)
# Usuario: REDACTED_USER | DB: Azure SQL
# =============================================================================
# Ejecutar UNA SOLA VEZ tras subir los scripts:
#   chmod +x setup.sh && sudo bash setup.sh
# =============================================================================

set -e

APP_DIR="/opt/renfe"

ENV_FILE="$(dirname "$0")/.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: No se encontró .env junto a setup.sh ($(realpath "$ENV_FILE"))"
    exit 1
fi
set -a; source "$ENV_FILE"; set +a

echo "========================================"
echo "  SETUP RENFE SCRAPER — Azure VM"
echo "========================================"

# ── 1. Actualizar sistema ─────────────────────────────────────────────────────
echo "[1/6] Actualizando sistema..."
apt-get update -qq && apt-get upgrade -y -qq

# ── 2. Instalar Python ────────────────────────────────────────────────────────
echo "[2/6] Instalando Python 3 y pip..."
apt-get install -y -qq python3 python3-pip curl gnupg2

# ── 3. Instalar ODBC Driver 18 para Azure SQL ────────────────────────────────
echo "[3/6] Instalando Microsoft ODBC Driver 18..."
curl -sSL https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl -sSL https://packages.microsoft.com/config/ubuntu/22.04/prod.list \
    > /etc/apt/sources.list.d/mssql-release.list
apt-get update -qq
ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev

# ── 4. Instalar dependencias Python ──────────────────────────────────────────
echo "[4/6] Instalando requests, pyodbc y python-dotenv..."
pip3 install --quiet requests pyodbc python-dotenv

# ── 5. Copiar scripts ─────────────────────────────────────────────────────────
echo "[5/6] Copiando scripts a $APP_DIR..."
mkdir -p "$APP_DIR"
cp /home/$VM_USER/renfe_capture.py "$APP_DIR/"
cp /home/$VM_USER/azure_db.py      "$APP_DIR/"
cp /home/$VM_USER/.env             "$APP_DIR/.env"
chmod 644 "$APP_DIR/.env"
chown -R $VM_USER:$VM_USER "$APP_DIR"

# Deshabilitar servicios antiguos si existieran
for svc in renfe-asturias renfe-cadiz renfe-largo; do
    if systemctl is-active --quiet "$svc" 2>/dev/null; then
        echo "  Deteniendo servicio antiguo: $svc"
        systemctl stop "$svc" || true
        systemctl disable "$svc" || true
    fi
done

# ── 6. Crear e iniciar servicio systemd unificado ────────────────────────────
echo "[6/6] Configurando servicio systemd renfe-capture..."

cat > /etc/systemd/system/renfe-capture.service << EOF
[Unit]
Description=Renfe Capture — Asturias + Cadiz + Largo Recorrido
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$VM_USER
WorkingDirectory=$APP_DIR
ExecStart=/usr/bin/python3 $APP_DIR/renfe_capture.py --loop 30 --flush-every 240
Restart=always
RestartSec=15

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable renfe-capture
systemctl start  renfe-capture

echo ""
echo "========================================"
echo "  INSTALACION COMPLETADA"
echo "========================================"
echo ""
systemctl is-active renfe-capture && echo "  renfe-capture : ACTIVO" || echo "  renfe-capture : ERROR"
echo ""
echo "  Cargar estaciones (solo la primera vez):"
echo "    python3 $APP_DIR/renfe_capture.py --init-stations"
echo ""
echo "  Ver logs en tiempo real:"
echo "    tail -f $APP_DIR/renfe-capture.log"
echo ""
echo "  Ver solo flushes a BD:"
echo "    grep FLUSH $APP_DIR/renfe-capture.log"
echo ""
echo "  Ver resumen de datos:"
echo "    python3 $APP_DIR/renfe_capture.py --summary"
echo "========================================"
