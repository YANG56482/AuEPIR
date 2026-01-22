#!/bin/bash

# AuEPIR Optimized Performance Script
# Optimized for 152-core machine
# "pt_tread" (Server Threads) set to 14 to maximize I/O throughput without context switching overhead

# 1. Threading Configuration
# OMP_NUM_THREADS=8 is optimal for 16 workers (Total 128 threads), leaving room for OS and Manager
export OMP_NUM_THREADS=8 

# 2. Experiment Parameters
CLIENTS=16
WORKERS=16
SERVER_THREADS=14  # High efficiency pt_tread
ADDRESS="inproc://server"
CONFIG_FILE="example/pir_configs.json"
ENABLE_AUEPIR=1    # Explicitly Enable Verification (User Request: 1=Open)

echo "========================================"
echo "Running AuEPIR Optimized Configuration"
echo "OMP Threads: $OMP_NUM_THREADS"
echo "Server Threads: $SERVER_THREADS"
echo "AuEPIR Verification: ENABLED ($ENABLE_AUEPIR)"
echo "========================================"

# 3. Clean Build (Ensuring no stale objects)
echo "[Script] Building..."
cmake --build build -- -j24 > /dev/null

# 4. Run Execution
echo "[Script] Executing..."
./bin/main_server $CONFIG_FILE $CLIENTS $WORKERS $SERVER_THREADS $ADDRESS $ENABLE_AUEPIR
