#include "factory.hpp"
#include "pir.hpp"

distribicom::Configs services::configurations::create_configs(std::uint64_t poly_deg, std::uint64_t logt, std::uint64_t rows, std::uint64_t cols,
                                                              std::uint64_t ele_size) {
    distribicom::Configs c;
    c.mutable_scheme()->assign("bgv");
    c.set_polynomial_degree(poly_deg);
    c.set_logarithm_plaintext_coefficient(logt);
    c.set_db_rows(rows);
    c.set_db_cols(cols);

    c.set_polynomial_degree(poly_deg);

    c.set_dimensions(2);
    c.set_size_per_element(ele_size);

    auto num_elements_in_ptx = elements_per_ptxt(logt, poly_deg, ele_size);
    auto num_elements = rows * cols * num_elements_in_ptx;
    c.set_number_of_elements(num_elements);
    c.set_use_batching(true);
    c.set_use_recursive_mod_switching(false);
    return c;
}

distribicom::AppConfigs
services::configurations::create_app_configs(const std::string &server_hostname, std::uint64_t poly_deg, std::uint64_t logt, std::uint64_t rows,
                                             std::uint64_t cols, std::uint64_t ele_size,
                                             std::uint64_t number_of_workers,
                                             std::uint64_t query_wait_time, std::uint64_t number_of_clients, std::uint64_t worker_step_size, double malicious_probability) {
    distribicom::AppConfigs c;
    c.mutable_configs()->CopyFrom(create_configs(poly_deg, logt, rows, cols, ele_size));
    c.mutable_configs()->set_malicious_probability(malicious_probability);
    c.mutable_configs()->set_worker_step_size(worker_step_size);
    c.mutable_main_server_hostname()->assign(server_hostname);
    c.set_number_of_workers(number_of_workers);
    c.set_query_wait_time(query_wait_time);
    c.set_number_of_clients(number_of_clients);

    return c;
}
//
//distribicom::WorkerConfigs
//services::configurations::create_worker_configs(const distribicom::AppConfigs &app_configs, int worker_port, std::string worker_hostname) {
//    distribicom::WorkerConfigs c;
//    c.set_workerport(worker_port);
//    c.set_worker_ip(worker_hostname);
//    c.mutable_appconfigs()->CopyFrom(app_configs);
//    return c;
//}

distribicom::ClientConfigs
services::configurations::create_client_configs(const distribicom::AppConfigs &app_configs, int client_port) {
    distribicom::ClientConfigs c;
    c.set_client_port(client_port);
    c.mutable_app_configs()->CopyFrom(app_configs);
    return c;
}
