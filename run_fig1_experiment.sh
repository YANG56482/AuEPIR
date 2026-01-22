#!/bin/bash

# Experiment Script for Figure 1: Scalability
# Settings: 
# 1. Clients=256  (2^8),  Workers=16  (2^4)
# 2. Clients=1024 (2^10), Workers=64  (2^6)
# 3. Clients=4096 (2^12), Workers=256 (2^8)

SERVER_THREADS=14
# Optimize for 152 cores: 16 Workers * 8 Threads = 128 Threads (Safe Saturation)
export OMP_NUM_THREADS=8 
ADDRESS="inproc://server"
CONFIG_FILE="example/pir_configs.json"

# Ensure malicious probability is 0 (Baseline)
perl -i -pe "s/\"malicious_probability\":\s*[0-9.]+/\"malicious_probability\": 0.0/" $CONFIG_FILE

# Initialize Data File
echo "Clients,Workers,TotalLatency(ms)" > fig1_data.csv

run_fig1_test() {
    clients=$1
    workers=$2
    
    echo "========================================"
    echo "Running Figure 1 Test"
    echo "Clients: $clients"
    echo "Workers: $workers"
    echo "========================================"
    
    # Run and Capture Output (Stream to stdout and temp file)
    ./bin/main_server $CONFIG_FILE $clients $workers $SERVER_THREADS $ADDRESS | tee server_output.tmp
    
    # Extract Latency (Assumes line "[DPIR] Total Latency:        XXXX.XX ms")
    latency=$(grep "Total Latency" server_output.tmp | awk '{print $4}')
    
    echo "$clients,$workers,$latency" >> fig1_data.csv
    echo "Saved: $clients,$workers,$latency"
    
    echo ""
    echo "Test Finished for Clients=$clients"
    echo ""
    sleep 2
}

# Scenario 1 (2^8)
run_fig1_test 256 16

# Scenario 2 (2^9)
# run_fig1_test 512 32

# Scenario 3 (2^10)
run_fig1_test 1024 64

# Scenario 4 (2^11)
# run_fig1_test 2048 128

# Scenario 5 (2^12)
run_fig1_test 4096 256

echo "Figure 1 Experiments Complete."
