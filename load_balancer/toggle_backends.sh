#!/usr/bin/env bash
# ================================================================
# toggle_backends.sh — Switch LB between single-backend and all-backends
# Uses paramiko-based Python helper. Run from project root.
#
# Usage:
#   bash toggle_backends.sh single   # Sys2 only
#   bash toggle_backends.sh all      # Sys2 + Sys3 + Sys4
# ================================================================
set -e

SYS1_PORT=2245
REMOTE_USER=student
REMOTE_HOST=10.1.75.51
REMOTE_DIR=/home/student/load_balancer

MODE=${1:-all}

generate_main_go() {
    local BACKENDS="$1"
    cat << GOEOF
package main

// AUTO-GENERATED — do not edit manually. Use toggle_backends.sh

import (
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net"
    "net/http"
    "sync"
    "sync/atomic"
    "time"

    "github.com/gorilla/websocket"
)

const listenAddr = ":3245"

var backendURLs = []string{
$BACKENDS
}

GOEOF
}

case $MODE in
    single)
        echo "🔧  Setting LB to SINGLE backend (Sys2 only)..."
        BACKENDS='    "ws://10.1.75.51:3246/ws", // Sys2 only'
        ;;
    all)
        echo "🔧  Setting LB to ALL THREE backends (Sys2 + Sys3 + Sys4)..."
        BACKENDS='    "ws://10.1.75.51:3246/ws", // Sys2
    "ws://10.1.75.51:3247/ws", // Sys3
    "ws://10.1.75.51:3248/ws", // Sys4'
        ;;
    *)
        echo "Usage: $0 [single|all]"
        exit 1
        ;;
esac

echo "===> Updating backend list on Sys1..."
# Patch backendURLs line in-place via SSH + sed
ssh -p $SYS1_PORT $REMOTE_USER@$REMOTE_HOST << ENDSSH
export PATH=\$PATH:/usr/local/go/bin
cd $REMOTE_DIR

# Use Python to patch the backendURLs in main.go
python3 << 'PYEOF'
import re

with open("main.go", "r") as f:
    content = f.read()

new_urls = """var backendURLs = []string{
$BACKENDS
}"""

content = re.sub(
    r'var backendURLs = \[\]string\{[^}]*\}',
    new_urls.strip(),
    content,
    flags=re.DOTALL
)

with open("main.go", "w") as f:
    f.write(content)

print("main.go patched successfully.")
PYEOF

# Rebuild
go build -o ws_load_balancer . && echo "Build OK"

# Restart
pkill -f ws_load_balancer 2>/dev/null || true
sleep 1
nohup ./ws_load_balancer > lb.log 2>&1 &
echo "LB restarted with PID \$!"
ENDSSH

echo "✅  Load balancer restarted in $MODE mode."
