#!/bin/bash
# =============================================================================
# sync_logs.sh — Descarga el log de captura desde la VM a tu PC local
# =============================================================================
# Requiere un fichero .env en la raíz del proyecto con VM_IP y VM_USER.
# Ejecutar desde tu PC (no desde la VM):
#   chmod +x deploy/sync_logs.sh && bash deploy/sync_logs.sh
# =============================================================================

ENV_FILE="$(dirname "$0")/../.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: No se encontró .env en $(realpath "$ENV_FILE")"
    exit 1
fi
set -a; source "$ENV_FILE"; set +a

SSH_KEY="deploy/RenfeKey.pem"
REMOTE_LOG="/opt/renfe/renfe-capture.log"
LOCAL_DIR="./logs"

mkdir -p "$LOCAL_DIR"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOCAL_FILE="$LOCAL_DIR/renfe-capture_$TIMESTAMP.log"

echo "Descargando log desde $VM_IP..."
scp -i "$SSH_KEY" \
    -o StrictHostKeyChecking=no \
    "$VM_USER@$VM_IP:$REMOTE_LOG" \
    "$LOCAL_FILE"

echo ""
echo "Log guardado en: $LOCAL_FILE"
echo ""

# Mostrar resumen rápido del log descargado
echo "=== Resumen ==="
echo "Total líneas    : $(wc -l < "$LOCAL_FILE")"
echo "Flushes a BD    : $(grep -c "FLUSH" "$LOCAL_FILE" || true)"
echo "Warnings/Errors : $(grep -cE "WARNING|ERROR" "$LOCAL_FILE" || true)"
echo "Primer registro : $(head -1 "$LOCAL_FILE")"
echo "Último registro : $(tail -1 "$LOCAL_FILE")"
