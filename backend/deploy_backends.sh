#!/usr/bin/env bash
# ================================================================
# deploy_backends.sh  — Deploy Flask chat backend to Sys2/3/4
#
# Usage: bash deploy_backends.sh [sys2|sys3|sys4|all]
# Example: bash deploy_backends.sh all
# ================================================================
set -e

REMOTE_USER=student
REMOTE_HOST=10.1.75.51
SOURCE_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# System → SSH port → app port (open port on each system)
declare -A SSH_PORTS=([sys2]=2246 [sys3]=2247 [sys4]=2248)
declare -A APP_PORTS=([sys2]=3246 [sys3]=3247 [sys4]=3248)

deploy_backend() {
    local SYS=$1
    local SSH_PORT=${SSH_PORTS[$SYS]}
    local APP_PORT=${APP_PORTS[$SYS]}
    local REMOTE_DIR="/home/student/grp-chat"

    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  Deploying backend to $SYS (SSH port $SSH_PORT, app port $APP_PORT)"
    echo "═══════════════════════════════════════════════════"

    echo "---> [1/4] Creating remote directory..."
    ssh -p $SSH_PORT $REMOTE_USER@$REMOTE_HOST "mkdir -p $REMOTE_DIR/static"

    echo "---> [2/4] Copying backend files..."
    scp -P $SSH_PORT \
        "$SOURCE_DIR/app.py" \
        "$SOURCE_DIR/db.py" \
        "$SOURCE_DIR/crypto_utils.py" \
        "$SOURCE_DIR/signatures.py" \
        "$SOURCE_DIR/integrity.py" \
        "$SOURCE_DIR/requirements.txt" \
        $REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR/

    # Copy static files if they exist
    if [ -d "$SOURCE_DIR/static" ]; then
        scp -P $SSH_PORT -r "$SOURCE_DIR/static/." $REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR/static/
        echo "---> Static files copied."
    fi

    echo "---> [3/4] Installing Python dependencies..."
    ssh -p $SSH_PORT $REMOTE_USER@$REMOTE_HOST << ENDSSH
cd $REMOTE_DIR
python3 -m venv venv 2>/dev/null || true
source venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt
echo "Dependencies installed."
ENDSSH

    echo "---> [4/4] Starting Flask backend on port $APP_PORT..."
    ssh -p $SSH_PORT $REMOTE_USER@$REMOTE_HOST << ENDSSH
cd $REMOTE_DIR
source venv/bin/activate

# Kill any existing instance on this port
fuser -k ${APP_PORT}/tcp 2>/dev/null || true
pkill -f "app.py" 2>/dev/null || true
sleep 1

export PORT=$APP_PORT
# Patch the port in app.py at runtime using env var
nohup python3 -c "
import os, importlib.util, sys
sys.path.insert(0, '.')
os.chdir('$REMOTE_DIR')

# Override port before importing app
import app as chat_app
chat_app.PORT = int(os.environ.get('PORT', 4000))
chat_app.app.run(host='0.0.0.0', port=chat_app.PORT, threaded=True)
" > backend.log 2>&1 &

echo "$SYS backend started with PID \$!"
echo "Accessible at: ws://10.1.75.51:${APP_PORT}/ws"
ENDSSH

    echo "✅  $SYS backend running on port $APP_PORT"
}

TARGET=${1:-all}

case $TARGET in
    sys2) deploy_backend sys2 ;;
    sys3) deploy_backend sys3 ;;
    sys4) deploy_backend sys4 ;;
    all)
        deploy_backend sys2
        deploy_backend sys3
        deploy_backend sys4
        echo ""
        echo "✅  All backends deployed!"
        echo "   Sys2: ws://10.1.75.51:3246/ws"
        echo "   Sys3: ws://10.1.75.51:3247/ws"
        echo "   Sys4: ws://10.1.75.51:3248/ws"
        ;;
    *)
        echo "Usage: $0 [sys2|sys3|sys4|all]"
        exit 1
        ;;
esac
