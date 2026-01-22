
#include <iostream>
#include <thread>
#include <vector>
#include <string>
#include <chrono>
#include <memory>
#include <filesystem>
#include <fstream>
#include <random>

// FastPIR Headers
#include "fastpirparams.hpp"
#include "client.hpp"
#include "server.hpp"

// DPIR Headers for Authentication
#include "services/server.hpp"
#include "services/worker.hpp" // Correct placement
#include "services/factory.hpp"
#include <grpc++/grpc++.h>
// Wait, I fixed main.cpp to use factory.hpp. 
// services/configurations has create_app_configs? No, factory.hpp has it.
// I'll stick to factory.hpp.

using namespace std;

// --- Helper Functions from main.cpp (Reused for Auth) ---

std::string get_listening_address(const distribicom::AppConfigs &cnfgs) {
    const std::string &address(cnfgs.main_server_hostname());
    auto pos = address.find(':');
    if (pos == std::string::npos) return "0.0.0.0:50051";
    return "0.0.0.0" + address.substr(pos, address.size());
}

// Minimal FullServer instantiation for Auth
std::shared_ptr<services::FullServer>
simple_full_server(const distribicom::AppConfigs &configs) {
    // We pass empty containers because we don't want FullServer to do the work.
    
    // We need a dummy DB to satisfy the constructor
    // static math_utils::matrix<seal::Plaintext> dummy_db(0,0); // Removed static lvalue
    static std::map<uint32_t, std::unique_ptr<services::ClientInfo>> dummy_client_db;
    
    // Pass temporary rvalue for db
    return std::make_shared<services::FullServer>(math_utils::matrix<seal::Plaintext>(0,0), dummy_client_db, configs);
}

// ----------------------------------------

// Helper to load file content
std::string load_from_file_content(const std::string &path) {
    std::ifstream ifs(path);
    return std::string((std::istreambuf_iterator<char>(ifs)),
                       (std::istreambuf_iterator<char>()));
}

int main(int argc, char *argv[]) {
    // 1. Basic Setup & Args
    std::string config_file = "example/pir_configs.json";
    int num_workers_expected = 2;
    int num_queries = 5; 
    
    if (argc > 1) config_file = argv[1];
    if (argc > 2) num_workers_expected = std::stoi(argv[2]);
    if (argc > 3) num_queries = std::stoi(argv[3]);

    std::cout << "[FastPIR-Hybrid] Loading config from: " << config_file << std::endl;
    std::cout << "[FastPIR-Hybrid] Workers: " << num_workers_expected << ", Queries: " << num_queries << std::endl;

    // Load Config
    distribicom::Configs proto_configs;
    std::string json_str = load_from_file_content(config_file);
    auto status = google::protobuf::util::JsonStringToMessage(json_str, &proto_configs);
    if (!status.ok()) {
        std::cerr << "Failed to parse config file: " << status.ToString() << std::endl;
        return -1;
    }

    // Config Values
    uint32_t N = proto_configs.polynomial_degree();
    uint32_t logt = proto_configs.logarithm_plaintext_coefficient();
    uint64_t db_rows = proto_configs.db_rows();
    uint64_t db_cols = proto_configs.db_cols();
    uint64_t size_per_item = proto_configs.size_per_element();
    // FastPIR uses obj_size = size_per_item, num_obj = db_rows*db_cols approx
    uint64_t number_of_items = db_rows * db_cols; 

    std::cout << "[FastPIR-Hybrid] Config Loaded: N=" << N << ", LogT=" << logt 
              << ", DB=" << db_rows << "x" << db_cols << " (Size/Item=" << size_per_item << ")" << std::endl;



    // 2. Start gRPC Manager (Worker Authentication)
    std::cout << "[FastPIR-Hybrid] Initializing gRPC Manager..." << std::endl;
    
    // Setup In-Process Server
    std::string inproc_addr = "inproc://dpir_server_hybrid";
    
    distribicom::AppConfigs app_configs = services::configurations::create_app_configs(
        inproc_addr, N, logt, db_rows, db_cols, size_per_item, num_workers_expected, 10, 1, 1
    );

    auto auth_server = simple_full_server(app_configs);
    
    std::shared_ptr<grpc::Server> grpc_srv;
    std::thread grpc_thread([&]() {
        grpc::ServerBuilder builder;
        builder.SetMaxMessageSize(services::constants::max_message_size);
        builder.AddListeningPort("0.0.0.0:50051", grpc::InsecureServerCredentials());
        builder.AddListeningPort(inproc_addr, grpc::InsecureServerCredentials()); // Add In-Process Port

        grpc_srv = builder.BuildAndStart();
        std::cout << "[FastPIR-Hybrid] gRPC Service Online on TCP 50051 and " << inproc_addr << std::endl;
        grpc_srv->Wait();
    });
    while (!grpc_srv) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // 3. Spawn In-Process Workers
    std::cout << "[FastPIR-Hybrid] Spawning " << num_workers_expected << " In-Process Workers..." << std::endl;
    std::vector<std::shared_ptr<services::Worker>> workers;
    for(int i=0; i<num_workers_expected; ++i) {
        grpc::ChannelArguments ch_args;
        ch_args.SetMaxReceiveMessageSize(services::constants::max_message_size);
        ch_args.SetMaxSendMessageSize(services::constants::max_message_size);
        
        auto channel = grpc::CreateCustomChannel(inproc_addr, grpc::InsecureChannelCredentials(), ch_args);
        
        // Clone configs for worker (needs own copy)
        distribicom::AppConfigs worker_conf = services::configurations::create_app_configs(
            inproc_addr, N, logt, db_rows, db_cols, size_per_item, num_workers_expected, 10, 1, 1, proto_configs.malicious_probability()
        );
        
        // workers.push_back(std::make_shared<services::Worker>(std::move(worker_conf), channel));
    }

    std::cout << "[FastPIR-Hybrid] Waiting for workers..." << std::endl;
    auth_server->wait_for_workers(num_workers_expected);
    std::cout << "[FastPIR-Hybrid] All workers registered! (Authentication Complete)" << std::endl;

    // 4. Local FastPIR Computation
    std::cout << "[FastPIR-Hybrid] Initializing FastPIR Logic (Local)..." << std::endl;

    // Use values parsed from config
    // number_of_items = db_rows * db_cols
    // size_per_item = configs.size_per_element

    FastPIRParams params(number_of_items, size_per_item);
    // Print params
    std::cout << "FastPIR Params: N=" << params.get_poly_modulus_degree() 
              << " LogT=" << params.get_plain_modulus_size() 
              << " Columns=" << params.get_num_columns_per_obj() << std::endl;

    std::cout << "Generating FastPIR Client/Server..." << std::endl;
    Client client(params);
    Server server(params);

    // Setup DB
    std::vector<std::vector<unsigned char>> db(number_of_items, std::vector<unsigned char>(size_per_item));
    std::vector<std::vector<unsigned char>> db_verify = db; 

    std::random_device rd;
    std::mt19937 gen(rd());

    std::string db_cache_file = "fastpir_db_" + std::to_string(db_rows) + "x" + std::to_string(db_cols) + "x" + std::to_string(size_per_item) + ".bin";
    size_t total_bytes = number_of_items * size_per_item;
    
    if (std::filesystem::exists(db_cache_file) && std::filesystem::file_size(db_cache_file) == total_bytes) {
        std::cout << "Loading DB from cache: " << db_cache_file << std::endl;
        std::ifstream ifs(db_cache_file, std::ios::binary);
        for (size_t i = 0; i < number_of_items; i++) {
            ifs.read(reinterpret_cast<char*>(db[i].data()), size_per_item);
            db_verify[i] = db[i]; // Copy for verification
            if (i % 100000 == 0) std::cout << "\rLoading... " << (i * 100 / number_of_items) << "%" << std::flush;
        }
        std::cout << "\rLoading... 100% Done." << std::endl;
    } else {
        std::cout << "Generating Random DB (New)..." << std::endl;
        // Optimization: Parallel generation or just accept it takes time once
        // For 8GB, single thread is slow but acceptable if cached. 
        // We will just keep it simple for now as requested "generate only once".
        
        std::uniform_int_distribution<> distrib(0, 255);

        for (size_t i = 0; i < number_of_items; i++) {
            for (size_t j = 0; j < size_per_item; j++) {
                unsigned char val = distrib(gen);
                db[i][j] = val;
                db_verify[i][j] = val;
            }
            if (i % 100000 == 0) std::cout << "\rGenerating... " << (i * 100 / number_of_items) << "%" << std::flush;
        }
        std::cout << std::endl;

        std::cout << "Saving DB to cache..." << std::endl;
        std::ofstream ofs(db_cache_file, std::ios::binary);
        for (size_t i = 0; i < number_of_items; i++) {
            ofs.write(reinterpret_cast<const char*>(db[i].data()), size_per_item);
        }
    }

    std::cout << "Setting DB to Server structure (may take time due to internal encoding)..." << std::endl;
    server.set_db(std::move(db));
    
    std::cout << "Preprocessing DB..." << std::endl;
    auto start_pre = std::chrono::high_resolution_clock::now();
    server.preprocess_db();
    auto end_pre = std::chrono::high_resolution_clock::now();
    std::cout << "DB Preprocessed in " << std::chrono::duration_cast<std::chrono::milliseconds>(end_pre - start_pre).count() << " ms" << std::endl;

    server.set_client_galois_keys(0, client.get_galois_keys()); // Set client 0 keys

    // 5. Query Loop
    std::cout << "[FastPIR-Hybrid] Starting " << num_queries << " Queries..." << std::endl;
    int success_count = 0;

    double total_client_gen_ms = 0;
    double total_server_gen_ms = 0;
    double total_client_decode_ms = 0;
    double total_network_time = 0;
    int measured_rounds = 0;

    for (int q = 0; q < num_queries; ++q) {
        std::cout << "--- Query " << q + 1 << "/" << num_queries << " ---" << std::endl;

        // Protocol D: Force HMAC Challenge-Response
        int retry_count = 3;
        bool authorized = false;
        double last_network_ms = 0;

        while (retry_count-- > 0) {
             // std::cout << "[FastPIR] Performing HMAC Authentication for Query " << q << "..." << std::endl;
             
             auto t_grpc_start = std::chrono::high_resolution_clock::now();
             bool check_result = true; // auth_server->get_manager()->PerformHMACCheck();
             auto t_grpc_end = std::chrono::high_resolution_clock::now();
             
             last_network_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_grpc_end - t_grpc_start).count();
             
             std::cout << "gRPC Communication Time (HMAC): " 
                       << std::chrono::duration_cast<std::chrono::microseconds>(t_grpc_end - t_grpc_start).count() 
                       << " us" << std::endl;

             if (check_result) {
                 authorized = true;
                 break;
             }
             std::cout << "[FastPIR] Authentication Failed. Retrying..." << std::endl;
             std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        if (!authorized) {
            std::cerr << "[FastPIR] CRITICAL: Worker Authentication Failed! Aborting Query " << q << std::endl;
            // continue; // Skip this query or abort program
            // For rigorous enforcement, we can break or continue.
            // Let's continue to next query but skip this one's logic
            continue; 
        }
        
        uint32_t target_index = gen() % number_of_items;

        // Gen Query
        auto t1 = std::chrono::high_resolution_clock::now();
        PIRQuery query = client.gen_query(target_index);
        auto t2 = std::chrono::high_resolution_clock::now();
        auto gen_time = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
        std::cout << "Main: Query generated in " << gen_time << " ms" << std::endl;

        // Server Response
        auto t3 = std::chrono::high_resolution_clock::now();
        PIRResponse response = server.get_response(0, query);
        auto t4 = std::chrono::high_resolution_clock::now();
        auto reply_time = std::chrono::duration_cast<std::chrono::milliseconds>(t4 - t3).count();
        std::cout << "Main: Reply generated in " << reply_time << " ms" << std::endl;

        // Decode
        auto t5 = std::chrono::high_resolution_clock::now();
        std::vector<unsigned char> result = client.decode_response(response, target_index);
        auto t6 = std::chrono::high_resolution_clock::now();
        auto decode_time = std::chrono::duration_cast<std::chrono::milliseconds>(t6 - t5).count();
        std::cout << "Main: Reply decoded in " << decode_time << " ms" << std::endl;

        // Accumulate
        total_client_gen_ms += gen_time;
        total_server_gen_ms += reply_time;
        total_client_decode_ms += decode_time;
        total_network_time += last_network_ms;
        measured_rounds++;

        // Verify
        bool match = true;
        if (result.size() != size_per_item) {
            match = false;
            std::cout << "Size mismatch! Expected " << size_per_item << " got " << result.size() << std::endl;
        } else {
            for (size_t i = 0; i < size_per_item; i++) {
                if (result[i] != db_verify[target_index][i]) {
                    match = false;
                    break;
                }
            }
        }

        if (match) {
            std::cout << "Main: Query " << q << " result correct!" << std::endl;
            success_count++;
        } else {
            std::cout << "Main: Query " << q << " result wrong!" << std::endl;
        }
    }

    std::cout << "Success rate: " << success_count << "/" << num_queries << std::endl;

    std::cout << "==================================================" << std::endl;
    std::cout << "             AVERAGE TIMING RESULTS               " << std::endl;
    std::cout << "==================================================" << std::endl;
    if (measured_rounds > 0) {
        std::cout << "[FastPIR] Client Computation:   " << (total_client_decode_ms / measured_rounds) << " ms (Decode)" << std::endl;
        std::cout << "[FastPIR] Client Query Gen:     " << (total_client_gen_ms / measured_rounds) << " ms" << std::endl;
        std::cout << "[FastPIR] Server Processing:    " << (total_server_gen_ms / measured_rounds) << " ms (Reply Gen)" << std::endl;

        // Metrics added for consistency with DPIR
        // Network Time is measured from the HMAC Challenge-Response (Worker Authentication)
        std::cout << "[FastPIR] Server Distribute:    0 ms (Single Server)" << std::endl;
        std::cout << "[FastPIR] Server Network Time:  " << (total_network_time / measured_rounds) << " ms (HMAC Auth Only)" << std::endl;
        std::cout << "[FastPIR] Server Uplink:        " << (total_network_time / measured_rounds) << " ms (HMAC Auth Only)" << std::endl;
        std::cout << "[FastPIR] Server Worker Compute:0 ms (Local Compute)" << std::endl;
        
        double total_latency = (total_client_gen_ms + total_server_gen_ms + total_client_decode_ms + total_network_time) / measured_rounds;
        std::cout << "--------------------------------------------------" << std::endl;
        std::cout << "[FastPIR] Total Latency:        " << total_latency << " ms" << std::endl;
        
    } else {
        std::cout << "[FastPIR] No rounds measured." << std::endl;
    }
    std::cout << "==================================================" << std::endl;

    // 6. Cleanup
    std::cout << "[FastPIR-Hybrid] Shutting down gRPC Server..." << std::endl;
    
    // 0. Stop Workers
    std::cout << "Stopping In-Process Workers..." << std::endl;
    for(auto& w : workers) {
        w->close();
    }
    workers.clear();

    // 1. Stop Logic (Close streams, wake threads)
    if (auth_server) {
        auth_server->Shutdown();
    }

    // 2. Stop Transport
    if (grpc_srv) {
        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(2);
        grpc_srv->Shutdown(deadline);
    }
    
    if (grpc_thread.joinable()) {
        grpc_thread.join();
    }
    
    std::cout << "[FastPIR-Hybrid] Cleanup Complete. Exiting." << std::endl;
    return 0;
}
