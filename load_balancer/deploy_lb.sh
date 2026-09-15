#!/usr/bin/env bash
# ================================================================
# deploy_lb.sh  — Deploy Go load balancer to Sys1
#
# Usage:  bash deploy_lb.sh
# ================================================================
set -e

SYS1_PORT=2245
REMOTE_USER=student
REMOTE_HOST=10.1.75.51
REMOTE_DIR=/home/student/load_balancer

echo "===> [1/4] Copying load balancer source to Sys1..."
ssh -p $SYS1_PORT $REMOTE_USER@$REMOTE_HOST "mkdir -p $REMOTE_DIR"
scp -P $SYS1_PORT go.mod go.sum main.go $REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR/

echo "===> [2/4] Installing Go on Sys1 (if not present)..."
ssh -p $SYS1_PORT $REMOTE_USER@$REMOTE_HOST << 'EOF'
if ! command -v go &>/dev/null; then
    echo "Go not found, installing..."
    cd /tmp
    wget -q https://go.dev/dl/go1.21.13.linux-amd64.tar.gz
    sudo tar -C /usr/local -xzf go1.21.13.linux-amd64.tar.gz
    echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
    export PATH=$PATH:/usr/local/go/bin
fi
go version
EOF

echo "===> [3/4] Building load balancer on Sys1..."
ssh -p $SYS1_PORT $REMOTE_USER@$REMOTE_HOST << 'EOF'
export PATH=$PATH:/usr/local/go/bin
cd /home/student/load_balancer
go mod download
go build -o ws_load_balancer .
echo "Build successful!"
EOF

echo "===> [4/4] Starting load balancer on Sys1 (port 3245)..."
ssh -p $SYS1_PORT $REMOTE_USER@$REMOTE_HOST << 'EOF'
export PATH=$PATH:/usr/local/go/bin
cd /home/student/load_balancer

# Kill any existing instance
pkill -f ws_load_balancer 2>/dev/null || true
sleep 1

# Start with nohup so it stays running after SSH disconnects
nohup ./ws_load_balancer > lb.log 2>&1 &
echo "Load balancer started with PID $!"
echo "Logs: /home/student/load_balancer/lb.log"
echo "Listening on: ws://10.1.75.51:3245/ws"
echo "Metrics:      http://10.1.75.51:3245/metrics"
echo "Health:       http://10.1.75.51:3245/health"
EOF

echo ""
echo "✅  Load Balancer deployed on Sys1!"
echo "   WebSocket : ws://10.1.75.51:3245/ws"
echo "   Metrics   : http://10.1.75.51:3245/metrics"
echo "   Health    : http://10.1.75.51:3245/health"
