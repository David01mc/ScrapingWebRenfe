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
VM_USER="REDACTED_USER"

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
echo "[4/6] Instalando requests y pyodbc..."
pip3 install --quiet requests pyodbc

# ── 5. Copiar scripts ─────────────────────────────────────────────────────────
echo "[5/6] Copiando scripts a $APP_DIR..."
mkdir -p "$APP_DIR"
cp /home/$VM_USER/renfe_asturias_cercanias.py   "$APP_DIR/"
cp /home/$VM_USER/renfe_cadiz_cercanias.py "$APP_DIR/"
cp /home/$VM_USER/renfe_largo_recorrido.py "$APP_DIR/"
cp /home/$VM_USER/azure_db.py              "$APP_DIR/"
chown -R $VM_USER:$VM_USER "$APP_DIR"

# ── 6. Crear e iniciar servicios systemd ─────────────────────────────────────
echo "[6/6] Configurando servicios systemd..."

cat > /etc/systemd/system/renfe-asturias.service << EOF
[Unit]
Description=Renfe Cercanias Asturias Capture
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$VM_USER
WorkingDirectory=$APP_DIR
ExecStart=/usr/bin/python3 $APP_DIR/renfe_asturias_cercanias.py --loop 30
Restart=always
RestartSec=10
StandardOutput=append:/var/log/renfe-asturias.log
StandardError=append:/var/log/renfe-asturias.log

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/renfe-cadiz.service << EOF
[Unit]
Description=Renfe Cercanias Cadiz Capture
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$VM_USER
WorkingDirectory=$APP_DIR
ExecStart=/usr/bin/python3 $APP_DIR/renfe_cadiz_cercanias.py --loop 30
Restart=always
RestartSec=10
StandardOutput=append:/var/log/renfe-cadiz.log
StandardError=append:/var/log/renfe-cadiz.log

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/renfe-largo.service << EOF
[Unit]
Description=Renfe Largo Recorrido Cadiz-Madrid Capture
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$VM_USER
WorkingDirectory=$APP_DIR
ExecStart=/usr/bin/python3 $APP_DIR/renfe_largo_recorrido.py --loop 30
Restart=always
RestartSec=10
StandardOutput=append:/var/log/renfe-largo.log
StandardError=append:/var/log/renfe-largo.log

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable renfe-asturias renfe-cadiz renfe-largo
systemctl start  renfe-asturias renfe-cadiz renfe-largo

echo ""
echo "========================================"
echo "  INSTALACION COMPLETADA"
echo "========================================"
echo ""
systemctl is-active renfe-asturias && echo "  renfe-asturias : ACTIVO" || echo "  renfe-asturias : ERROR"
systemctl is-active renfe-cadiz    && echo "  renfe-cadiz    : ACTIVO" || echo "  renfe-cadiz    : ERROR"
systemctl is-active renfe-largo    && echo "  renfe-largo    : ACTIVO" || echo "  renfe-largo    : ERROR"
echo ""
echo "  Cargar estaciones (solo la primera vez):"
echo "    python3 $APP_DIR/renfe_largo_recorrido.py --init-stations"
echo ""
echo "  Ver logs en tiempo real:"
echo "    journalctl -u renfe-asturias -f"
echo "    journalctl -u renfe-cadiz    -f"
echo "    journalctl -u renfe-largo    -f"
echo "========================================"
