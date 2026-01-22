#include "../test_utils.hpp"
#include "worker.hpp"
#include "factory.hpp"
#include "server.hpp"
#include <grpc++/grpc++.h>
#include <latch>

#define NUM_CLIENTS 64
constexpr std::string_view server_port = "5051";
constexpr std::string_view worker_port = "52100";

std::thread runFullServer(std::latch &wg, services::FullServer &f);

std::thread setupWorker(std::latch &wg, distribicom::AppConfigs &configs, std::shared_ptr<services::Manager> mgr);

services::FullServer
full_server_instance(std::shared_ptr<TestUtils::CryptoObjects> &all, const distribicom::AppConfigs &configs);

void sleep(int n_seconds) { std::this_thread::sleep_for(std::chrono::milliseconds(n_seconds * 1000)); }

int worker_test(int, char *[]) {
    auto all = TestUtils::setup(TestUtils::DEFAULT_SETUP_CONFIGS);

    auto cfgs = services::configurations::create_app_configs(
        "localhost:" + std::string(server_port),
        int(all->encryption_params.poly_modulus_degree()),
        20,
        42,
        41,
        256,
        1,
        10,
        NUM_CLIENTS,
        1
    );

#ifdef FREIVALDS
    std::cout << "Running with FREIVALDS!!!!" << std::endl;
#else
    std::cout << "Running without FREIVALDS!!!!" << std::endl;
#endif
    std::cout << cfgs.DebugString() << std::endl;
    std::cout << "total queries: " << NUM_CLIENTS << "\n\n" << std::endl;
    services::FullServer fs = full_server_instance(all, cfgs);

    std::latch wg(1);

    std::vector<std::thread> threads;
    threads.emplace_back(runFullServer(wg, fs));

    std::cout << "setting up manager and client services." << std::endl;
    sleep(3);

    std::cout << "setting up worker-service" << std::endl;
    distribicom::AppConfigs moveable_appcnfgs;
    moveable_appcnfgs.CopyFrom(cfgs);
    std::shared_ptr<services::Manager> mgr = fs.get_manager();
    threads.emplace_back(setupWorker(wg, moveable_appcnfgs, mgr));

    std::vector<std::chrono::microseconds> results;
    fs.wait_for_workers(1);
    fs.start_epoch();
    for (int i = 0; i < 10; ++i) {
        auto time_round_s = std::chrono::high_resolution_clock::now();

        auto ledger = fs.distribute_work(i);
        ledger->done.read();
        fs.run_step_2(ledger);

        auto time_round_e = std::chrono::high_resolution_clock::now();
        auto time_round_ms = duration_cast<std::chrono::milliseconds>(time_round_e - time_round_s).count();
        std::cout << "Main_Server: round " << i << " running time: " << time_round_ms << " ms" << std::endl;
        results.emplace_back(time_round_ms);
    }

    std::cout << "results: [ ";
    for (std::uint64_t i = 0; i < results.size() - 1; ++i) {
        std::cout << results[i].count() << "ms, ";
    }
    std::cout << results.back().count() << "ms ]" << std::endl;

    sleep(5);
    std::cout << "\nshutting down.\n" << std::endl;
    wg.count_down();
    for (auto &t: threads) {
        t.join();
    }
    sleep(5);
    return 0;
}


std::map<uint32_t, std::unique_ptr<services::ClientInfo>>
create_client_db(int size, std::shared_ptr<TestUtils::CryptoObjects> &all, const distribicom::AppConfigs &app_configs) {
    auto m = marshal::Marshaller::Create(all->encryption_params);
    std::map<uint32_t, std::unique_ptr<services::ClientInfo>> cdb;
    for (int i = 0; i < size; i++) {
        auto gkey = all->gal_keys;
        auto gkey_serialised = m->marshal_seal_object(gkey);
        std::vector<std::vector<seal::Ciphertext>> query = {{all->random_ciphertext()},
                                                            {all->random_ciphertext()}};
        distribicom::ClientQueryRequest query_marshaled;
        m->marshal_query_vector(query, query_marshaled);
        auto client_info = std::make_unique<services::ClientInfo>(services::ClientInfo());

        services::set_client(math_utils::compute_expansion_ratio(all->seal_context.first_context_data()->parms()) * 2,
                             app_configs.configs().db_rows(), i, gkey, gkey_serialised, query, query_marshaled,
                             client_info);

        cdb.insert(
            {i, std::move(client_info)});
    }
    return cdb;
}

services::FullServer
full_server_instance(std::shared_ptr<TestUtils::CryptoObjects> &all, const distribicom::AppConfigs &configs) {
    auto num_clients = NUM_CLIENTS;
    math_utils::matrix<seal::Plaintext> db(configs.configs().db_rows(), configs.configs().db_cols());

    for (auto &p: db.data) {
        p = all->random_plaintext();
    }

    auto cdb = create_client_db(num_clients, all, configs);

    return services::FullServer(std::move(db), cdb, configs);
}


// assumes that configs are not freed until we copy it inside the thread!
std::thread setupWorker(std::latch &wg, distribicom::AppConfigs &configs, std::shared_ptr<services::Manager> mgr) {
    return std::thread([&, mgr] {
        try {
            auto worker = std::make_shared<services::Worker>(std::move(configs));
            
            // Connect Shared Memory
            worker->set_manager(mgr);
            mgr->RegisterWorker(worker);

            wg.wait();
            
            // In Shared Memory, we usually run_session() or similar?
            // worker->run_session(); // This blocks?
            // In test, maybe we just wait?
            // The Original test just waited on latch.
            // But we need to process tasks!
            // Let's spawn a thread for run_session?
            
            std::thread processing_thread([worker](){
                worker->run_session();
            });
            
            // Wait for test completion (wg)
            // But we are ALREADY in a thread here (setupWorker returns thread).
            // wg.wait() blocks this thread.
            
            processing_thread.join(); // run_session loops until closed?
            
            worker->close();
        } catch (std::exception &e) {
            std::cerr << "setupWorker :: exception: " << e.what() << std::endl;
        }
    });
}

std::thread runFullServer(std::latch &wg, services::FullServer &f) {
    return std::thread([&] {
        std::cout << "manager and client services are active (Shared Memory)" << std::endl;
        wg.wait();

        f.close();
    });
}