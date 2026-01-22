[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.11080231.svg)](https://doi.org/10.5281/zenodo.11080232)

# AuEPIR

**AuEPIR** (formerly Distribicom) is a high-performance, privacy-preserving communications library. It serves as a Proof of Concept (POC) for a fully untrusted communication system where workers are not trusted by the main server.

> [!WARNING]
> AuEPIR is research code and is **not intended for production use**.

## Prerequisites

* **OS**: Ubuntu 22.04 (Tested).
* **Compiler**: GCC 11.2+ or Clang 14.0+ (C++20 required).
* **Build System**: CMake 3.20+.

## Installation

### Option 1: Quick Build (Vendored Dependencies)

```bash
cmake -S . -B build
cmake --build build --target all -j 4
```

### Option 2: Pre-installed Dependencies

1. **Install Dependencies**:

    ```bash
    ./scripts/compile_install_scripts/deps.sh
    ```

2. **Build**:

    ```bash
    cmake -S . -B build \
          -D CMAKE_PREFIX_PATH=<dependency_install_location> \
          -D USE_PRECOMPILED_SEAL=ON \
          -D USE_PREINSTALLED_GRPC=ON
    cmake --build build --target all -j 4
    ```

## Usage

AuEPIR supports two modes of operation. You can use the provided helper scripts or run the binaries manually.

### Helper Scripts (Recommended)

* **Standard AuEPIR**:

    ```bash
    ./run_dis.sh <num_workers> <num_queries>
    ```

* **FastPIR Hybrid**:

    ```bash
    ./run_fastpir.sh <num_workers> <num_queries>
    ```

---

### Manual Execution

Executables are located in `bin/`.

#### Mode 1: Standard AuEPIR

This is the standard distributed PIR mode.

**1. Run Server:**

```bash
./bin/main_server <pir_config_file> <num_queries> <num_workers> <num_server_threads> <hostname:port>
```

* `pir_config_file`: JSON config (e.g., `example/pir_configs.json`).
* `num_queries`: Number of PIR queries to process.
* `num_workers`: Number of expected workers.
* `num_server_threads`: Threads for server computation.
* `hostname:port`: Address to listen on. Can be a TCP address (e.g., `0.0.0.0:50051`) or an in-process address (e.g., `"inproc://server"`).

**Example (TCP):**

```bash
./bin/main_server example/pir_configs.json 1 2 4 0.0.0.0:50051
```

**Example (In-Process):**

```bash
./bin/main_server example/pir_configs.json 16 2 8 "inproc://server"
```

**2. Run Workers:**

```bash
./bin/worker <pir_config_file> <num_worker_threads> <server_address:port>
```

* If using in-process communication, ensure the address matches (e.g., `"inproc://server"`).

#### Mode 2: FastPIR Hybrid

A hybrid mode leveraging FastPIR optimizations.

**1. Run Server:**

```bash
./bin/main_fastpir <pir_config_file> <num_workers> <num_queries>
```

* **Note**: The argument order differs from `main_server`.

**2. Run Workers:**
Use the same `bin/worker` binary as above.

```bash
./bin/worker <pir_config_file> <num_worker_threads> <server_address:port>
```

## Configuration

The system uses a JSON configuration file. See `example/pir_configs.json` for a template. Key parameters include `db_rows`, `db_cols`, `scheme` (e.g., "bgv"), and `polynomial_degree`.

## Project Structure

* **`src/`**: Source code.
* **`test/`**: Unit tests (`ctest` to run).
* **`scripts/`**: Build and helper scripts.
