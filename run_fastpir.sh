#!/bin/bash

# =========================================================================
# 1. Proxy Fix
# =========================================================================
# unset http_proxy
# unset https_proxy
# unset HTTP_PROXY
# unset HTTPS_PROXY
# unset all_proxy
# unset ALL_PROXY
# export no_proxy="localhost,127.0.0.1,0.0.0.0,::1"
# export NO_PROXY="localhost,127.0.0.1,0.0.0.0,::1"

# =========================================================================
# 2. Build Project
# =========================================================================
echo "========================================"
echo "Step 1: Building Project (FastPIR - Clean Build)..."
echo "========================================"

PROJECT_ROOT=$(pwd)

# Build with CMake
cmake -S . -B build -DCMAKE_PREFIX_PATH=$HOME/.distribicom_installs -DCMAKE_BUILD_TYPE=Release
if [ $? -ne 0 ]; then
    echo "Error: CMake configuration failed!"
    exit 1
fi

# Build Project
echo "Building..."
cmake --build build -- -j152

if [ $? -ne 0 ]; then
    echo "Error: Build failed!"
    exit 1
fi

cd "$PROJECT_ROOT" || exit 1

# =========================================================================
# 3. LOCATE BINARIES
# =========================================================================
SERVER_BIN=$(find ./bin ./build -name "main_fastpir*" -type f 2>/dev/null | head -n 1)
WORKER_BIN=$(find ./bin ./build -name "worker*" -type f 2>/dev/null | grep -v "worker_bench" | head -n 1)

if [ -z "$SERVER_BIN" ]; then
    echo "Error: Server binary (main_fastpir) not found!"
    exit 1
fi

if [ -z "$WORKER_BIN" ]; then
    echo "Error: Worker binary (worker) not found!"
    exit 1
fi

# Default Args
WORKERS=${1:-2}
QUERIES=${2:-5}
CONFIG_FILE="./example/pir_configs.json"
SERVER_ADDR="0.0.0.0:50051"

echo "Found Server (FastPIR Hybrid): $SERVER_BIN"
echo "Found Worker: $WORKER_BIN"

# Cleanup function
cleanup() {
    echo "Stopping background processes..."
    if [ -n "$SERVER_PID" ]; then kill $SERVER_PID 2>/dev/null; wait $SERVER_PID 2>/dev/null; fi
    if [ -n "${WORKER_PIDS[*]}" ]; then
        for pid in "${WORKER_PIDS[@]}"; do
            kill $pid 2>/dev/null
        done
        wait "${WORKER_PIDS[@]}" 2>/dev/null
    fi
}
trap cleanup EXIT SIGINT

# =========================================================================
# 4. RUN TEST
# =========================================================================
echo "========================================"
echo "Step 2: Starting FastPIR Hybrid Test"
echo "========================================"
echo "Cleaning up old processes..."
# Windows/Linux cleanup compatibility
taskkill //F //IM main_fastpir.exe > /dev/null 2>&1
taskkill //F //IM worker.exe > /dev/null 2>&1
pkill -f main_fastpir > /dev/null 2>&1
pkill -f worker > /dev/null 2>&1
sleep 1

# Start Server
echo "Starting Server: $SERVER_BIN"
# FastPIR Main usage: <config> <workers> <queries>
$SERVER_BIN "$CONFIG_FILE" "$WORKERS" "$QUERIES" &
SERVER_PID=$!

sleep 2

# Start Workers (With CPU Pinning)
WORKER_PIDS=()
WORKER_CORES_PER_NODE=9
WORKER_THREADS_ARG=8

for ((i=1; i<=WORKERS; i++)); do
    START_CORE=$(( (i-1) * WORKER_CORES_PER_NODE ))
    END_CORE=$(( START_CORE + WORKER_CORES_PER_NODE - 1 ))
    CPU_RANGE="$START_CORE-$END_CORE"
    
    echo "Starting Worker $i (Cores: $CPU_RANGE, Threads: $WORKER_THREADS_ARG)..."
    
    if command -v taskset &> /dev/null; then
        taskset -c $CPU_RANGE $WORKER_BIN "$CONFIG_FILE" "$WORKER_THREADS_ARG" "$SERVER_ADDR" > "worker_${i}.log" 2>&1 &
    else
        echo "[WARN] 'taskset' not found. Running without pinning."
        $WORKER_BIN "$CONFIG_FILE" "$WORKER_THREADS_ARG" "$SERVER_ADDR" > "worker_${i}.log" 2>&1 &
    fi
    WORKER_PIDS+=($!)
    sleep 0.5
done

echo "--------------------------------------------------"
echo "Running... Check console for output."
echo "Waiting for exit..."
echo "--------------------------------------------------"

wait $SERVER_PID
