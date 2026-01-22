
// forcing rebuild
#include "services/server.hpp"
#include "services/client_context.hpp"


services::FullServer::FullServer(math_utils::matrix<seal::Plaintext> &&db, std::map<uint32_t,
    std::unique_ptr<services::ClientInfo>> &client_db,
                                 const distribicom::AppConfigs &app_configs) :
    pir_configs(app_configs.configs()),
    enc_params(utils::setup_enc_params(app_configs)) {
    
    manager = std::make_shared<services::Manager>(app_configs, client_db, std::move(db));
    init_pir_data(app_configs);

}

void services::FullServer::init_pir_data(const distribicom::AppConfigs &app_configs) {
    const auto &configs = app_configs.configs();
    gen_pir_params(configs.number_of_elements(), configs.size_per_element(),
                   configs.dimensions(), enc_params, pir_params, configs.use_symmetric(),
                   configs.use_batching(), configs.use_recursive_mod_switching());
}


void services::FullServer::Shutdown() {
    manager->Shutdown();
}

std::shared_ptr<services::WorkDistributionLedger> services::FullServer::distribute_work(std::uint64_t round) {
    std::shared_lock client_db_lock(*(manager->client_query_manager.mutex));
    return manager->distribute_work(manager->client_query_manager, round, 1);
}

void services::FullServer::start_epoch(int round, bool clear_blacklist) {
    std::shared_lock client_db_lock(*manager->client_query_manager.mutex);

    manager->new_epoch(manager->client_query_manager, round, clear_blacklist);
    manager->send_galois_keys(manager->client_query_manager);
}

void services::FullServer::wait_for_workers(int i) {
    manager->wait_for_workers(i);
}

void services::FullServer::publish_galois_keys() {
    throw std::logic_error("not implemented");
}

void services::FullServer::publish_answers() {
    throw std::logic_error("not implemented");
}

void services::FullServer::send_stop_signal() {
    manager->close();
}

void services::FullServer::learn_about_rouge_workers(std::shared_ptr<WorkDistributionLedger>) {
#ifdef FREIVALDS
    manager->wait_on_verification();
#endif
}

void services::FullServer::run_step_2(std::shared_ptr<WorkDistributionLedger>) {
    auto time = utils::time_it([&]() {
        manager->calculate_final_answer();
    });
    std::cout << "Running step 2: " << time << " ms" << std::endl;

}

void services::FullServer::tell_new_round() {
    throw std::logic_error("not implemented");
}
