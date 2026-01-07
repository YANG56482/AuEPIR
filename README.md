[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.11080231.svg)](https://doi.org/10.5281/zenodo.11080232)

# Distribicom: A communications library that achieves low communication costs and high performance.

Distribicom is a research library and should not be used in production systems. 
Distribicom is a POC of a fully untrusted communication system with workers that are fully untrusted by the main server.

# Compiling Distribicom
Distribicom is written in C++20. and in particular requires GCC 11.2 or later or clang 14.0 or later. We have tested our
code on ubuntu 22.04 and cannot guarantee test results on other OSes or versions. We assume that you have gcc/clang and cmake 3.20+ installed

## Compilation instructions

Distribicom is a ```cmake``` project. To compile it with vendored dependencies, from the project root directory do the following:

```PowerShell
cmake -S . -B build
cmake --build build --target all -j 4
```

Alternatively, if you plan on building Distribicom many times, you can install the dependencies once, and compile as follows:

run [./scripts/compile_install_scripts/deps.sh](/scripts/compile_install_scripts/deps.sh) documentation as to where and how the dependencies are installed is in the script file.
Next run

```PowerShell
 cmake -S . -B build \
                  -D CMAKE_PREFIX_PATH=<dependency_install_location> \
                   -D USE_PRECOMPILED_SEAL=ON -D USE_PREINSTALLED_GRPC=ON
 cmake --build build --target all -j 4
```

We recommend the first method if you do not plan on compiling Distribicom multiple times.

## Running Distribicom

The first step is to launch the server:

```PowerShell
 ./bin/main_server <pir_config_file> <num_queries> <num_workers> <num_server_threads> <your_hostname:port_to_listen_on>
```

The next step is to launch the workers:

```PowerShell
 ./bin/worker <pir_config_file> <num_worker_threads> <server_address:server_listening_port>
```

A logfile with round timings by the server will be written in the directory from which you launched the server. An example run with an example
```pir_config_file``` can be found in the [/example](/example) directory


# Testing
Distribicom has extensive unit tests which can be found in [/test](/test). Compiled tests can be found under the [/bin](/bin) directory and can be run with ```ctest```

# dpir
