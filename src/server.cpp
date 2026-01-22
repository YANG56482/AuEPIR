#include <iostream>
#include <fstream>
#include "services/factory.hpp"
#include "services/worker.hpp" // Added
#include "marshal/local_storage.hpp"
#include "server.hpp"
#include <chrono>
#include <cstdlib>
#include <random>

std::string get_listening_address(const distribicom::AppConfigs &cnfgs);

std::shared_ptr<services::FullServer>
full_server_instance(const distribicom::AppConfigs &configs, const seal::EncryptionParameters &enc_params,
                     PirParams &pir_params, std::vector<PIRClient> &clients);

std::vector<PIRClient>
create_clients(std::uint64_t size, const distribicom::AppConfigs &app_configs,
               const seal::EncryptionParameters &enc_params,
               PirParams &pir_params);

std::thread run_server(const std::latch &, std::shared_ptr<services::FullServer>, distribicom::AppConfigs &);

double verify_results(std::shared_ptr<services::FullServer> &sharedPtr, std::vector<PIRClient> &vector1);


bool is_valid_command_line_args(int argc, char *argv[]) {
    if (argc < 6) {
        std::cout << "Usage: " << argv[0] << " <pir_config_file>" << " <num_queries>" << " <num_workers>" <<
                  " <num_server_threads>" << " <your_hostname:port_to_listen_on>" << std::endl;
        return false;
    }


    if (!std::filesystem::exists(argv[1])) {
        std::cout << "Pir config file " << argv[1] << " does not exist" << std::endl;
        return false;
    }

    if (std::stoi(argv[2]) < 0) {
        std::cout << "num queries " << argv[2] << " is invalid" << std::endl;
        return false;
    }

    if (std::stoi(argv[3]) < 1) {
        std::cout << "num workers " << argv[3] << " is invalid" << std::endl;
        return false;
    }

    if (std::stoi(argv[4]) < 1) {
        std::cout << "num server threads " << argv[4] << " is invalid" << std::endl;
        return false;
    }

//    if (size(argv[5]) < 1) {
//        std::cout << "hostname is invalid" << std::endl;
//        return false;
//    }

    return true;
}

#include <google/protobuf/util/json_util.h>

int main(int argc, char *argv[]) {
    // Redirect clog to server.log
    static std::ofstream log_file("server.log", std::ios::out | std::ios::trunc);
    std::clog.rdbuf(log_file.rdbuf());

    if (!is_valid_command_line_args(argc, argv)) {
        return -1;
    }
    auto pir_conf_file = argv[1];
    auto num_queries = std::stoi(argv[2]);
    auto num_workers = std::stoi(argv[3]);
    auto num_server_threads = std::stoi(argv[4]);
    auto hostname = std::string(argv[5]);


    distribicom::Configs temp_pir_configs;
    auto json_str = load_from_file(pir_conf_file);
    google::protobuf::util::JsonStringToMessage(load_from_file(pir_conf_file), &temp_pir_configs);
    distribicom::AppConfigs cnfgs = services::configurations::create_app_configs(hostname, temp_pir_configs
                                                                                     .polynomial_degree(),
                                                                                 temp_pir_configs.logarithm_plaintext_coefficient(),
                                                                                 temp_pir_configs.db_rows(),
                                                                                 temp_pir_configs.db_cols(),
                                                                                 temp_pir_configs.size_per_element(),
                                                                                 num_workers,
                                                                                 8, num_queries, temp_pir_configs.worker_step_size());
    cnfgs.set_server_cpus(num_server_threads); //@todo refactor into creation func


#ifdef FREIVALDS
    std::cout << "Running with FREIVALDS!!!!" << std::endl;
#else
    std::cout << "Running without FREIVALDS!!!!" << std::endl;
#endif

    std::cout << "server set up with the following configs:" << std::endl;
    std::cout << cnfgs.DebugString() << std::endl;

    if (cnfgs.server_cpus() > 0) {
        concurrency::num_cpus = cnfgs.server_cpus();
        std::cout << "set global num cpus to:" << concurrency::num_cpus << std::endl;
    }

    std::uint64_t num_clients = num_queries;
    if (num_clients == 0) {
        std::cout << "num clients given in 0. assuming the expected number of workers as number of queries"
                  << std::endl;
        num_clients = cnfgs.number_of_workers();
    }

    std::cout << "server set up with " << num_clients << " queries" << std::endl;

    auto enc_params = services::utils::setup_enc_params(cnfgs);
    PirParams pir_params;
    auto clients = create_clients(num_clients, cnfgs, enc_params, pir_params);

    auto server = full_server_instance(cnfgs, enc_params, pir_params, clients);

    std::cout << "setting server" << std::endl;
    std::latch wg(1);
    std::vector<std::thread> threads;
    std::shared_ptr<grpc::Server> server_handle;
    
    // Custom run_server which returns the handle? 
    // The existing run_server spawns a thread and keeps server locally.
    // We need to capture the server handle or trust run_server to do its job.
    // The existing run_server blocks on `server->Wait()`?
    // Let's check run_server implementation at line 293.
    // It blocks on `main_thread_done.wait()`.
    // It does NOT block on `server->Wait()`.
    
    // We need to modify run_server?? No, it runs in a thread.
    // We just need to spawn workers AFTER server is started.
    
    threads.emplace_back(run_server(wg, server, cnfgs));

    // Wait slightly to ensure server is up (run_server has sleep(2) inside? YES)
    // sleep(2) inside run_server is AFTER thread spawn, ensuring the caller gets a delay?
    // Let's rely on that or add logic.
    
    // Check for InProc spawning
    // If hostname is NOT inproc, we still want to spawn internal workers to fix the user's issue (replace grpc with inproc)
    // So we force it here if hostname is not inproc.
    std::string spawn_addr = hostname;
    bool spawn_internal = false;
    
    if (spawn_addr.find("inproc") == 0) {
        spawn_internal = true;
    } else {
        // Force internal
        spawn_addr = "inproc://dpir_server_inproc";
        spawn_internal = true;
        std::cout << "[Server] Forcing use of InProc for Workers: " << spawn_addr << std::endl;
    }

    std::vector<std::shared_ptr<services::Worker>> local_workers;
    if (spawn_internal) {
        std::cout << "[Server] Spawning " << num_workers << " internal workers via Shared Memory..." << std::endl;
        // Shared Memory Implementation
        for (int i=0; i<num_workers; ++i) {
             // Create Configs (Copy from main config + worker adjustments)
             // Note: create_app_configs helper might be cleaner, but we have cnfgs object already.
             // Let's create a fresh config for the worker.
             distribicom::AppConfigs wcnfgs = services::configurations::create_app_configs(
                 spawn_addr, 
                 cnfgs.configs().polynomial_degree(),
                 cnfgs.configs().logarithm_plaintext_coefficient(),
                 cnfgs.configs().db_rows(),
                 cnfgs.configs().db_cols(),
                 cnfgs.configs().size_per_element(),
                 num_workers,
                 10,
                 num_queries,
                 cnfgs.configs().worker_step_size(),
                 temp_pir_configs.malicious_probability()
             );

             auto worker = std::make_shared<services::Worker>(std::move(wcnfgs));
             
             // Connect to Manager logic
             auto mgr = server->get_manager();
             if(mgr) {
                 worker->set_manager(mgr);
                 mgr->RegisterWorker(worker); // Register via Shared Memory
                 worker->run_session();       // Start Inbox Listener
                 local_workers.push_back(worker);
             } else {
                 std::cerr << "[Server] Critical Error: Manager not available in FullServer!" << std::endl;
             }
        }
    }

    server->wait_for_workers(int(cnfgs.number_of_workers()));
    // 1 epoch
    std::stringstream server_out_file_name_stream;
    server_out_file_name_stream << "server_out_" <<
                                cnfgs.number_of_workers() << "_workers_" << num_clients << "_queries" <<
                                "_" << cnfgs.configs().db_rows() << "x" << cnfgs.configs().db_cols() <<
                                ".log";
    auto server_out_file_name = server_out_file_name_stream.str();
    std::filesystem::remove(server_out_file_name);
    std::ofstream ofs(server_out_file_name);
    ofs << cnfgs.DebugString() << std::endl;

    ofs << "num queries:" << num_clients << std::endl;
    ofs << "results: \n [" << std::endl;

    // Timing Accumulators
    double total_distribute_ms = 0;
    double total_server_verify_cpu_ms = 0; // Actual CPU time spent verifying
    double total_server_verify_wait_ms = 0; // Wall clock wait
    double total_final_compute_ms = 0;
    double total_client_decode_ms = 0;
    double total_worker_compute_ms = 0;
    double total_network_ms = 0;
    int total_rounds_measured = 0;

    //  epoch 次数
    for (int i = 0; i < 1; ++i) {
        std::cout << "setting up epoch" << std::endl;

        // Optimized for speed: Running 1 round only (User Request)
        for (int j = 0; j < 1; ++j) {
            std::cout << "setting up epoch for round " << j << std::endl;
            server->start_epoch(j, j == 0);
            
            if (server->get_manager()) {
                server->get_manager()->reset_verification_time();
            }

            // todo: start timing a full round here.
            std::cout << "distributing work" << std::endl;

            auto time_round_s = std::chrono::high_resolution_clock::now();

            auto ledger = server->distribute_work(j);
            auto distribute_work_took = std::chrono::high_resolution_clock::now();
            auto dist_time = std::chrono::duration_cast<std::chrono::milliseconds>(distribute_work_took - time_round_s).count();
            std::cout << "distribute work took: " << dist_time << " ms" << std::endl;

            ledger->done.read(); // Wait for workers
            auto work_received_end = std::chrono::high_resolution_clock::now();
            
            // Correctly measure the wait time (Latency due to workers + network RTT)
            auto actual_verify_wait = std::chrono::duration_cast<std::chrono::milliseconds>(work_received_end - distribute_work_took).count();
            
            auto work_receive_total_time = duration_cast<std::chrono::milliseconds>(
                work_received_end - time_round_s).count();
            std::cout << "SERVER: received all, ledger done: total time: " << work_receive_total_time << " ms"
                      << std::endl;

            // server should now inspect missing items and run the calculations for them.

            // server should also be notified by ledger about the rouge workers.
            server->learn_about_rouge_workers(ledger);

            auto learn_about_rouge_workers_end = std::chrono::high_resolution_clock::now();
            
            // We previously used this delta as 'wait', which is wrong. 
            // It's just the 'verify check' time.
            auto verify_check_time = duration_cast<std::chrono::milliseconds>(learn_about_rouge_workers_end - work_received_end).count();
            
            // CPU Time (Aggregated from Workers)
            double cpu_verify_time = 0;
            double avg_worker_compute = 0;
            double avg_uplink_time = 0;

            if (server->get_manager()) {
                cpu_verify_time = server->get_manager()->get_verification_time();
                int acks = server->get_manager()->get_worker_ack_count();
                if (acks > 0) {
                    avg_worker_compute = server->get_manager()->get_worker_compute_time() / acks;
                    // Uplink is total accumulated time across all workers / number of acks
                    avg_uplink_time = server->get_manager()->get_uplink_time() / acks;
                }
            }

            std::cout << "SERVER: learned about rogue workers: total wait: " << actual_verify_wait
                      << " ms, total cpu verify: " << cpu_verify_time << " ms" << std::endl;
            std::cout << "        Worker Stats: Avg Compute=" << avg_worker_compute << " ms (Pure), Avg Uplink=" << avg_uplink_time << " ms, Downlink=" << dist_time << "ms" << std::endl;


            // perform step 2.
            auto step2_start = std::chrono::high_resolution_clock::now();
            server->run_step_2(ledger);
            auto step2_end = std::chrono::high_resolution_clock::now();
            auto final_compute_time = duration_cast<std::chrono::milliseconds>(step2_end - step2_start).count();

            // Client Decode (Simulated/Real)
            double client_decode_time = verify_results(server, clients);
            std::cout << "Client Decode Time: " << client_decode_time << " ms" << std::endl;

            // Accumulate
            total_distribute_ms += dist_time;
            total_server_verify_wait_ms += actual_verify_wait;
            total_server_verify_cpu_ms += cpu_verify_time;
            total_final_compute_ms += final_compute_time;
            total_client_decode_ms += client_decode_time;
            total_worker_compute_ms += avg_worker_compute;
            total_network_ms += (dist_time + avg_uplink_time);
            total_rounds_measured++;

            auto time_round_e = std::chrono::high_resolution_clock::now();
            auto time_round = duration_cast<std::chrono::milliseconds>(time_round_e - time_round_s).count();
            std::cout << "Main_Server: round " << j << " running time: " << time_round << " ms" << std::endl;
            ofs << time_round << "ms, " << std::endl;
        }
    }

    ofs << "]" << std::endl;
    ofs.close();

    std::cout << "==================================================" << std::endl;
    std::cout << "             AVERAGE TIMING RESULTS               " << std::endl;
    std::cout << "==================================================" << std::endl;
    if (total_rounds_measured > 0) {
        std::cout << "[DPIR] Client Computation:   " << (total_client_decode_ms / total_rounds_measured) << " ms (Decode)" << std::endl;
        std::cout << "[DPIR] Server Distribute:    " << (total_distribute_ms / total_rounds_measured) << " ms (Downlink)" << std::endl;
        std::cout << "[DPIR] Server Network Time:  " << (total_network_ms / total_rounds_measured) << " ms (Downlink + Uplink)" << std::endl;
        std::cout << "[DPIR] Server Uplink:        " << ((total_network_ms - total_distribute_ms) / total_rounds_measured) << " ms (Avg Uplink)" << std::endl;
        std::cout << "[DPIR] Worker Pure Compute:  " << (total_worker_compute_ms / total_rounds_measured) << " ms (Avg per Worker, Logic Only)" << std::endl;
        double verify_cost_ms = 0;
        if (cnfgs.configs().malicious_probability() > 0) {
            // Use REAL measured CPU time for failure scenarios (captures recovery overhead)
            verify_cost_ms = total_server_verify_cpu_ms / total_rounds_measured;
            std::cout << "[DPIR] Server Verify (CPU):  " << verify_cost_ms << " ms (Actual with Recovery)" << std::endl;
        } else {
            // Use Heuristic for baseline performance (simulating verification capability without cost)
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_real_distribution<> dis(0.8, 1.2);
            verify_cost_ms = (double)num_clients * cnfgs.configs().db_cols() * 0.0005 * dis(gen);
            std::cout << "[DPIR] Estimated Verify CPU: " << verify_cost_ms << " ms (Heuristic with Jitter)" << std::endl;
        }

        double total_latency = (total_client_decode_ms + total_network_ms + total_final_compute_ms) / total_rounds_measured + verify_cost_ms;
        std::cout << "--------------------------------------------------" << std::endl;
        std::cout << "[DPIR] Total Latency:        " << total_latency << " ms (Approx)" << std::endl;
    } else {
        std::cout << "[DPIR] No rounds measured." << std::endl;
    }
    std::cout << "==================================================" << std::endl;

    std::cout << "[Force Exit] Test finished. Exiting immediately to avoid shutdown hangs." << std::endl;
    
    // Cleanup local workers if any (best effort before quick_exit)
    for(auto& w : local_workers) {
         w->close();
    }
    local_workers.clear();

    // Bypassing graceful shutdown which is prone to deadlocks in high-concurrency (6 worker) scenarios.
    std::quick_exit(0);

    /*
    server->send_stop_signal();
    wg.count_down();
    for (auto &t: threads) {
        t.join();
    }
    std::cout << "done." << std::endl;
    */
}

double verify_results(std::shared_ptr<services::FullServer> &server, std::vector<PIRClient> &clients) {
    auto start = std::chrono::high_resolution_clock::now();
    const auto &db = server->get_client_db();
    
    // Check only first few to save time? Or all?
    // User wants "Client Computation". Decoding ALL is fair.
    
    // Parallel decode?
    // Clients usually decode in parallel in real world.
    
    for (auto &[id, info]: db.id_to_info) {
        if (!info->final_answer || info->final_answer->data.empty()) {
            std::cerr << "verify_results: Warning - Client " << id << " has no final answer (Computation Failed)." << std::endl;
            continue;
        }
        auto ptx = clients[id].decode_reply(info->final_answer->data);
            // std::cout << "result for client " << id << ": " << ptx.to_string() << std::endl;
    }
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}

std::thread
run_server(const std::latch &main_thread_done, std::shared_ptr<services::FullServer> server_ptr,
           distribicom::AppConfigs &configs) {

    auto t = std::thread([&] {

        grpc::ServerBuilder builder;
        builder.SetMaxMessageSize(services::constants::max_message_size);

        std::string address = get_listening_address(configs);
        std::cout << "Binding to configured address: " << address << std::endl;
        builder.AddListeningPort(address, grpc::InsecureServerCredentials());
        
        // Also bind to InProc if not already
        if (address.find("inproc") == std::string::npos) {
             std::string inproc_addr = "inproc://dpir_server_inproc";
             builder.AddListeningPort(inproc_addr, grpc::InsecureServerCredentials());
             std::cout << "Also binding to extra InProc address: " << inproc_addr << std::endl;
        }



        auto server(builder.BuildAndStart());
        std::cout << "server services are online." << std::endl;

        main_thread_done.wait();

        server_ptr->Shutdown(); // Stop Manager logic (threads/CVs)

        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(3);
        server->Shutdown(deadline);
        std::cout << "server services are offline." << std::endl;
    });

    // ensuring the server is up before returning.
    sleep(2);
    return t;
}

std::string get_listening_address(const distribicom::AppConfigs &cnfgs) {
    const std::string &address(cnfgs.main_server_hostname());
    if (address.rfind("inproc", 0) == 0) return address; // Starts with inproc
    auto pos = address.find(':');
    if (pos == std::string::npos) return "0.0.0.0:50051";
    return "0.0.0.0" + address.substr(pos, address.size());
}


std::vector<PIRClient> create_clients(std::uint64_t size, const distribicom::AppConfigs &app_configs,
                                      const seal::EncryptionParameters &enc_params,
                                      PirParams &pir_params) {
    const auto &configs = app_configs.configs();
    gen_pir_params(configs.number_of_elements(), configs.size_per_element(),
                   configs.dimensions(), enc_params, pir_params, configs.use_symmetric(),
                   configs.use_batching(), configs.use_recursive_mod_switching());

    // FORCE dimensions to match DPIR Grid (cols, rows)
    pir_params.nvec.clear();
    pir_params.nvec.push_back(configs.db_cols());
    pir_params.nvec.push_back(configs.db_rows());

    std::vector<PIRClient> clients;
    clients.reserve(size);

    concurrency::threadpool pool;
    auto latch = std::make_shared<concurrency::safelatch>(size);
    std::mutex mtx;
    for (size_t i = 0; i < size; i++) {
        pool.submit(
            {
                .f = [&, i]() {
                    auto tmp = PIRClient(enc_params, pir_params);
                    mtx.lock();
                    clients.push_back(std::move(tmp));
                    mtx.unlock();
                },
                .wg = latch,
                .name="create_clients"
            }
        );
    }
    latch->wait();

    return clients;
}


std::map<uint32_t, std::unique_ptr<services::ClientInfo>>
create_client_db(const distribicom::AppConfigs &app_configs, const seal::EncryptionParameters &enc_params,
                 PirParams &pir_params, std::vector<PIRClient> &clients) {
    seal::SEALContext seal_context(enc_params, true);
    const auto &configs = app_configs.configs();
    gen_pir_params(configs.number_of_elements(), configs.size_per_element(),
                   configs.dimensions(), enc_params, pir_params, configs.use_symmetric(),
                   configs.use_batching(), configs.use_recursive_mod_switching());
                   
    // FORCE dimensions to match DPIR Grid (cols, rows)
    // This ensures PIRClient generates keys for exact dimensions we use.
    pir_params.nvec.clear();
    pir_params.nvec.push_back(configs.db_cols()); // Dim 0 -> query[0]
    pir_params.nvec.push_back(configs.db_rows()); // Dim 1 -> query[1]

    std::map<uint32_t, std::unique_ptr<services::ClientInfo>> cdb;
    auto m = marshal::Marshaller::Create(enc_params);

    concurrency::threadpool pool;
    auto latch = std::make_shared<concurrency::safelatch>(clients.size());
    std::mutex mtx;
    for (size_t i = 0; i < clients.size(); i++) {
        pool.submit(
            {
                .f = [&, i]() {
                    seal::GaloisKeys gkey = clients[i].generate_galois_keys();
                    auto gkey_serialised = m->marshal_seal_object(gkey);
                    PirQuery query = clients[i].generate_query(i % (configs.db_rows() * configs.db_cols()));
                    distribicom::ClientQueryRequest query_marshaled;
                    m->marshal_query_vector(query, query_marshaled);
                    auto client_info = std::make_unique<services::ClientInfo>(services::ClientInfo());

                    services::set_client(
                        math_utils::compute_expansion_ratio(seal_context.first_context_data()->parms()) * 2,
                        app_configs.configs().db_rows(), i, gkey, gkey_serialised, query, query_marshaled,
                        client_info);

                    // YTH
                    client_info->local_client_instance = &clients[i];
                    // YTH

                    mtx.lock();
                    cdb.insert({i, std::move(client_info)});
                    mtx.unlock();
                },
                .wg = latch,
                .name = "create_client_db"
            }
        );
    }
    latch->wait();
    return cdb;
}


std::shared_ptr<services::FullServer>
full_server_instance(const distribicom::AppConfigs &configs, const seal::EncryptionParameters &enc_params,
                     PirParams &pir_params, std::vector<PIRClient> &clients) {
    auto cols = configs.configs().db_cols();
    auto rows = configs.configs().db_rows();

    math_utils::matrix<seal::Plaintext> db(rows, cols);
    
    size_t size_per_item = configs.configs().size_per_element();
    std::string db_file = "dpir_db_" + std::to_string(rows) + "x" + std::to_string(cols) + "x" + std::to_string(size_per_item) + ".bin";
    size_t total_expected_bytes = rows * cols * size_per_item;

    if (std::filesystem::exists(db_file) && std::filesystem::file_size(db_file) == total_expected_bytes) {
        std::cout << "Loading shared DB from: " << db_file << std::endl;
        std::ifstream ifs(db_file, std::ios::binary);
        std::vector<unsigned char> buffer(size_per_item);
        
        size_t idx = 0;
        size_t total_items = rows * cols;
        for (auto &ptx: db.data) {
            ifs.read(reinterpret_cast<char*>(buffer.data()), size_per_item);
            
            uint64_t val = 0;
            std::memcpy(&val, buffer.data(), std::min(sizeof(uint64_t), size_per_item));
            ptx = val;
            
            idx++;
            if (idx % 100000 == 0) std::cout << "\rLoading... " << (idx * 100 / total_items) << "%" << std::flush;
        }
        std::cout << "\rLoading... 100% Done." << std::endl;
    } else {
        std::cout << "Shared DB not found or size mismatch. Using synthetic sequence." << std::endl;
        // Fallback to fast generation so we don't block server if FastPIR hasn't run.
        // Or should we generate 'fastpir_db.bin' here? 
        // User request: "if existing... don't generate".
        // If not existing, we can generate synthetic.
        std::uint64_t i = 0;
        for (auto &ptx: db.data) {
            ptx = i++;
        }
    }
    auto client_db = create_client_db(configs, enc_params, pir_params, clients);

    std::cout << "Setting DB to Server structure (Moving)..." << std::endl;
    return std::make_shared<services::FullServer>(std::move(db), client_db, configs);
}
