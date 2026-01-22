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
echo "Step 1: Building Project (Clean Build)..."
echo "========================================"

# Store current directory
PROJECT_ROOT=$(pwd)

# DPIR uses its own thread pool, so disable Global OpenMP auto-threading to avoid contention
export OMP_NUM_THREADS=1


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

# Return to project root
cd "$PROJECT_ROOT" || exit 1

# =========================================================================
# 3. Run Test
# =========================================================================

# Default values
WORKERS=${1:-2}
QUERIES=${2:-1}

# Paths (Relative to project root)
SERVER_BIN="./bin/main_server"
WORKER_BIN="./bin/worker"
CONFIG_FILE="./example/pir_configs.json"
SERVER_ADDR="0.0.0.0:50051"

# Check executables logic
# Search in both bin (standard output) and build (fallback)
SERVER_BIN=$(find ./bin ./build -name "main_server*" -type f 2>/dev/null | head -n 1)
WORKER_BIN=$(find ./bin ./build -name "worker*" -type f 2>/dev/null | grep -v "worker_bench" | head -n 1)

if [ -z "$SERVER_BIN" ]; then
    echo "Error: Server binary (main_server) not found in ./bin or ./build!"
    exit 1
fi

if [ -z "$WORKER_BIN" ]; then
    echo "Error: Worker binary (worker) not found in ./bin or ./build!"
    exit 1
fi

echo "Found Server: $SERVER_BIN"
echo "Found Worker: $WORKER_BIN"

# Cleanup function to kill child processes on exit
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

echo "========================================"
echo "Step 2: Starting Test with $WORKERS Workers"
echo "Cleaning up old processes..."
taskkill //F //IM main_server.exe > /dev/null 2>&1
taskkill //F //IM worker.exe > /dev/null 2>&1
# Fallback for linux-like envs
pkill -f main_server > /dev/null 2>&1
pkill -f worker > /dev/null 2>&1
sleep 1

# echo "Killing old processes..."
# pkill -f main_server
# pkill -f worker
# sleep 1
echo "Server: $SERVER_BIN"
echo "Config: $CONFIG_FILE"
echo "========================================"

# Start Server in background
$SERVER_BIN "$CONFIG_FILE" "$QUERIES" "$WORKERS" 2 "$SERVER_ADDR" &
SERVER_PID=$!

# Give server time to initialize
sleep 2

# Start Workers (With CPU Pinning)
WORKER_PIDS=()

# Config: 9 Cores per Worker (8 Compute + 1 Mgmt)
# Adjust these based on your NUMA topology
WORKER_CORES_PER_NODE=9
WORKER_THREADS_ARG=8

# for ((i=1; i<=WORKERS; i++)); do
#     # Calculate CPU Range
#     # Worker 1: 0-8
#     # Worker 2: 9-17
#     START_CORE=$(( (i-1) * WORKER_CORES_PER_NODE ))
#     END_CORE=$(( START_CORE + WORKER_CORES_PER_NODE - 1 ))
#     CPU_RANGE="$START_CORE-$END_CORE"
#     
#     echo "Starting Worker $i (Cores: $CPU_RANGE, Threads: $WORKER_THREADS_ARG)..."
#     
#     # Check if taskset exists (Linux)
#     if command -v taskset &> /dev/null; then
#         taskset -c $CPU_RANGE $WORKER_BIN "$CONFIG_FILE" "$WORKER_THREADS_ARG" "$SERVER_ADDR" &
#     else
#         # Fallback for Windows (Start /affinity could work but requires hex mask, simplistic fallback here)
#         echo "[WARN] 'taskset' not found. Running without pinning."
#         $WORKER_BIN "$CONFIG_FILE" "$WORKER_THREADS_ARG" "$SERVER_ADDR" &
#     fi
#     WORKER_PIDS+=($!)
#     sleep 0.5
# done

echo "--------------------------------------------------"
echo "All processes running. Waiting for exit..."
echo "--------------------------------------------------"

# Wait for Server to exit (Graceful Shutdown)
wait $SERVER_PID
EXIT_CODE=$?
echo "Server exited with code $EXIT_CODE"

# Wait for Workers
for pid in "${WORKER_PIDS[@]}"; do
    wait $pid
done
echo "All workers exited."
echo "Test Complete."
echo "NOTE: External workers were disabled in this script because Server (main_server/main_fastpir) now spawns In-Process Workers internally."
