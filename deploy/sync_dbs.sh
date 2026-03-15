#!/bin/bash
# =============================================================================
# sync_dbs.sh — Descarga las bases de datos desde la VM a tu PC local
# =============================================================================
# Requiere un fichero .env en la raíz del proyecto con VM_IP y VM_USER.
# Ejecutar desde tu PC (no desde la VM):
#   chmod +x sync_dbs.sh && ./sync_dbs.sh
# =============================================================================

ENV_FILE="$(dirname "$0")/../.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: No se encontró .env en $(realpath "$ENV_FILE")"
    exit 1
fi
set -a; source "$ENV_FILE"; set +a

SSH_KEY="~/RenfeKey.pem"    # Clave descargada al crear la VM en Azure
REMOTE_DIR="/opt/renfe"
LOCAL_DIR="./dbs_backup"

# ─────────────────────────────────────────────────────────────────────────────


mkdir -p "$LOCAL_DIR"

echo "Descargando bases de datos desde $VM_IP..."

for db in renfe_asturias.db renfe_cadiz.db renfe_largo_recorrido.db; do
    echo "  → $db"
    scp -i "$SSH_KEY" \
        -o StrictHostKeyChecking=no \
        "$VM_USER@$VM_IP:$REMOTE_DIR/$db" \
        "$LOCAL_DIR/$db"
done

echo ""
echo "Listo. Bases de datos en: $LOCAL_DIR/"
ls -lh "$LOCAL_DIR/"
