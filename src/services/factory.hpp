#pragma once

#include "distribicom.pb.h"


namespace services::configurations {

    distribicom::Configs create_configs(std::uint64_t poly_deg, std::uint64_t logt, std::uint64_t rows, std::uint64_t cols, std::uint64_t ele_size);

    distribicom::AppConfigs
    create_app_configs(const std::string &server_hostname, std::uint64_t poly_deg, std::uint64_t logt, std::uint64_t rows, std::uint64_t cols, std::uint64_t
    ele_size, std::uint64_t number_of_workers, std::uint64_t query_wait_time, std::uint64_t number_of_clients, std::uint64_t worker_step_size, double malicious_probability = 0.0);

//    distribicom::WorkerConfigs create_worker_configs(const distribicom::AppConfigs &app_configs, int worker_port, std::string worker_hostname);
//
    distribicom::ClientConfigs create_client_configs(const distribicom::AppConfigs &app_configs, int client_port);
}
