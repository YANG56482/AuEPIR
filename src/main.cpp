#include "pir_client.hpp"
#include "pir_server.hpp"
#include <seal/seal.h>
#include <chrono>
#include <memory>
#include <random>
#include <cstdint>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>
#include <latch>

#include "services/server.hpp"
#include "services/worker.hpp"
#include "services/factory.hpp"
#include "services/utils.hpp" // Added include
#include <google/protobuf/util/json_util.h>

using namespace std::chrono;
using namespace std;
using namespace seal;

// --- Helpers ---

std::string load_from_file_content(const std::string &path) {
    std::ifstream ifs(path);
    return std::string((std::istreambuf_iterator<char>(ifs)),
                       (std::istreambuf_iterator<char>()));
}

// Minimal FullServer instantiation
std::shared_ptr<services::FullServer>
simple_full_server(const distribicom::AppConfigs &configs) {
    static std::map<uint32_t, std::unique_ptr<services::ClientInfo>> dummy_client_db;
    return std::make_shared<services::FullServer>(math_utils::matrix<seal::Plaintext>(0,0), dummy_client_db, configs);
}

int main(int argc, char *argv[]) {
    // Usage: ./main1 <config_file> <num_workers> <num_queries>
    std::string config_file = "example/pir_configs.json";
    int num_workers_expected = 2;
    int num_queries = 5; 

    if (argc > 1) config_file = argv[1];
    if (argc > 2) num_workers_expected = std::stoi(argv[2]);
    if (argc > 3) num_queries = std::stoi(argv[3]);

    std::cout << "[SharedMem] Loading config from: " << config_file << std::endl;
    std::cout << "[SharedMem] Workers: " << num_workers_expected << ", Queries: " << num_queries << std::endl;

    // Load Config
    distribicom::Configs proto_configs;
    std::string json_str = load_from_file_content(config_file);
    auto status = google::protobuf::util::JsonStringToMessage(json_str, &proto_configs);
    if (!status.ok()) {
        std::cerr << "Failed to parse config file: " << status.ToString() << std::endl;
        return -1;
    }

    uint32_t N = proto_configs.polynomial_degree();
    uint32_t logt = proto_configs.logarithm_plaintext_coefficient();
    uint64_t db_rows = proto_configs.db_rows();
    uint64_t db_cols = proto_configs.db_cols();
    uint64_t size_per_item = proto_configs.size_per_element();
    uint64_t number_of_items = db_rows * db_cols; 

    uint32_t d = proto_configs.dimensions(); 
    bool use_symmetric = proto_configs.use_symmetric(); 
    bool use_batching = proto_configs.use_batching(); 
    bool use_recursive_mod_switching = proto_configs.use_recursive_mod_switching();

    std::cout << "[SharedMem] Params from Config: d=" << d 
              << ", sym=" << use_symmetric 
              << ", batch=" << use_batching 
              << ", rec_mod=" << use_recursive_mod_switching << std::endl;

    std::cout << "[SharedMem] Config Loaded: N=" << N << ", LogT=" << logt 
              << ", DB=" << db_rows << "x" << db_cols << " (Size/Item=" << size_per_item << ")" << std::endl;

    // Create AppConfigs FIRST to generate correct params
    distribicom::AppConfigs app_configs = services::configurations::create_app_configs(
        "shared_memory_ignored", N, logt, db_rows, db_cols, size_per_item, num_workers_expected, 10, 1, 1
    );
    
    // OVERWRITE defaults with loaded config values to ensure consistency
    app_configs.mutable_configs()->set_dimensions(proto_configs.dimensions());
    app_configs.mutable_configs()->set_use_symmetric(proto_configs.use_symmetric());
    app_configs.mutable_configs()->set_use_batching(proto_configs.use_batching());
    app_configs.mutable_configs()->set_use_recursive_mod_switching(proto_configs.use_recursive_mod_switching());


    // 1. Encryption Params (Synced with Manager)
    EncryptionParameters enc_params = services::utils::setup_enc_params(app_configs);
    PirParams pir_params;
    
    // gen_encryption_params(N, logt, enc_params); // Skipped: Using utils::setup_enc_params
    verify_encryption_params(enc_params);
    gen_pir_params(number_of_items, size_per_item, d, enc_params, pir_params, use_symmetric, use_batching, use_recursive_mod_switching);

    // --- DEBUG LOGGING ---
    auto pid = enc_params.parms_id();
    std::cout << "[Main] Enc Params ParmsID: " << pid[0] << "-" << pid[1] << "-" << pid[2] << "-" << pid[3] << std::endl; 
    std::cout << "[Main] Params Detail: N=" << enc_params.poly_modulus_degree() 
              << ", CoeffModSize=" << enc_params.coeff_modulus().size()
              << ", PlainMod=" << enc_params.plain_modulus().value() << std::endl;
    // ---------------------

    pir_params.nvec.clear();
    pir_params.nvec.push_back(db_cols);
    pir_params.nvec.push_back(db_rows);

    // 2. Start Manager (Shared Memory Mode)
    std::cout << "[SharedMem] Initializing Manager..." << std::endl;

    // app_configs already created

    auto auth_server = simple_full_server(app_configs);
    auto manager = auth_server->get_manager();

    // 3. Spawn Workers and Connect via Shared Memory
    std::cout << "[SharedMem] Spawning " << num_workers_expected << " Workers via Shared Memory..." << std::endl;
    std::vector<std::shared_ptr<services::Worker>> workers;
    
    for(int i=0; i<num_workers_expected; ++i) {
        distribicom::AppConfigs worker_conf = services::configurations::create_app_configs(
            "shared_memory_ignored", N, logt, db_rows, db_cols, size_per_item, num_workers_expected, 10, 1, 1
        );
        
        auto w = std::make_shared<services::Worker>(std::move(worker_conf));
        
        // Connect Manager <-> Worker
        manager->RegisterWorker(w);
        w->set_manager(manager->weak_from_this());
        w->run_session(); // Starts inbox listener
        
        workers.push_back(w);
    }
    
    manager->wait_for_workers(num_workers_expected);
    std::cout << "[SharedMem] All workers registered and connected!" << std::endl;

    // 4. Local PIR Computation (Original main.cpp Logic)
    std::cout << "[SharedMem] Proceeding with Local Calculation (Ignoring Workers for PIR calc)..." << std::endl;

    PIRClient client(enc_params, pir_params);
    GaloisKeys galois_keys = client.generate_galois_keys();

    PIRServer server(enc_params, pir_params);
    server.set_galois_key(0, galois_keys);

    auto db(make_unique<uint8_t[]>(number_of_items * size_per_item));
    auto db_copy(make_unique<uint8_t[]>(number_of_items * size_per_item));
    std::string db_file = "sealpir_db_" + std::to_string(db_rows) + "x" + std::to_string(db_cols) + "x" + std::to_string(size_per_item) + ".bin";
    size_t total_bytes = number_of_items * size_per_item;
    
    seal::Blake2xbPRNGFactory factory;
    auto gen = factory.create();
    
    if (std::filesystem::exists(db_file) && std::filesystem::file_size(db_file) == total_bytes) {
        std::cout << "Main: Loading shared DB from: " << db_file << std::endl;
        std::ifstream ifs(db_file, std::ios::binary);
        for (uint64_t i = 0; i < number_of_items; i++) {
            ifs.read(reinterpret_cast<char*>(&db.get()[i * size_per_item]), size_per_item);
            std::memcpy(&db_copy.get()[i * size_per_item], &db.get()[i * size_per_item], size_per_item);
        }
    } else {
        std::cout << "Main: Generating shared DB (New)..." << std::endl;
        for (uint64_t i = 0; i < number_of_items; i++) {
            for (uint64_t j = 0; j < size_per_item; j++) {
                auto val = gen->generate() % 256;
                db.get()[(i * size_per_item) + j] = val;
                db_copy.get()[(i * size_per_item) + j] = val;
            }
        }
        std::ofstream ofs(db_file, std::ios::binary);
        ofs.write(reinterpret_cast<const char*>(db.get()), total_bytes);
    }

    server.set_database(move(db), number_of_items, size_per_item);
    server.preprocess_database();

    std::cout << "[SharedMem] Starting " << num_queries << " Queries Loop..." << std::endl;
    int success_count = 0;
    int measured_rounds = 0;
    double total_gen_time = 0;
    double total_decode_time = 0;
    double total_network_time = 0;

    for (int q = 0; q < num_queries; ++q) {
        std::cout << "--- Query " << q + 1 << "/" << num_queries << " ---" << std::endl;

        uint64_t ele_index = gen->generate() % number_of_items;
        uint64_t index = client.get_fv_index(ele_index);
        uint64_t offset = client.get_fv_offset(ele_index);

        // Client: Generate Query
        std::cout << "[SharedMem] Client Generating Query..." << std::endl;
        auto t_gen_start = std::chrono::high_resolution_clock::now();
        PirQuery query = client.generate_query(index);
        auto t_gen_end = std::chrono::high_resolution_clock::now();
        total_gen_time += std::chrono::duration_cast<std::chrono::milliseconds>(t_gen_end - t_gen_start).count();
        std::cout << "[Trace] Query Generated. Size: " << query.size() << std::endl;
        
        // --- DEBUG LOGGING ---
        if (!query.empty()) {
             if(!query[0].empty()) {
                 auto qpid = query[0][0].parms_id();
                 std::cout << "[Main] Query[0][0] ParmsID: " << qpid[0] << "-" << qpid[1] << "-" << qpid[2] << "-" << qpid[3] << std::endl;
             }
             if(query.size() > 1 && !query[1].empty()) {
                  auto qpid = query[1][0].parms_id();
                  std::cout << "[Main] Query[1][0] ParmsID: " << qpid[0] << "-" << qpid[1] << "-" << qpid[2] << "-" << qpid[3] << std::endl;
             }
        }
        // ---------------------
        
        // Register Query in Manager (via ClientDB)
        auto& client_db = auth_server->get_client_db();
        // Clear previous query info (simple simulation for ID 0)
        // In this loop we simulate "new" queries. 
        // We'll treat query 'q' as client ID 'q' for simplicity if we want parallel queries,
        // or just reuse ID 0. Ideally, Manager supports client IDs.
        // Let's use ID 0 and reset it.
        uint32_t client_id = 0; 
        
        {
             // Reset client info for reuse
             client_db.id_to_info[client_id] = std::make_unique<services::ClientInfo>();
             client_db.id_to_info[client_id]->query = query;
             client_db.id_to_info[client_id]->galois_keys = galois_keys;
             client_db.id_to_info[client_id]->local_client_instance = &client; // For verification inside Manager
             
             // --- SERIALIZATION FOR WORKER ---
             // 1. Serialize Galois Keys
             std::cout << "[Trace] Serializing GKeys..." << std::endl;
             std::stringstream gkey_ss;
             galois_keys.save(gkey_ss, seal::compr_mode_type::none); 

             client_db.id_to_info[client_id]->galois_keys_marshaled = std::make_unique<distribicom::WorkerTaskPart>();
             client_db.id_to_info[client_id]->galois_keys_marshaled->mutable_gkey()->set_keys(gkey_ss.str());
             client_db.id_to_info[client_id]->galois_keys_marshaled->mutable_gkey()->set_key_pos(client_id); 
             
             // 2. Serialize Query (Assuming 1 CT for simple case, or just the first one as primary)
             std::cout << "[Trace] Serializing Query..." << std::endl;
             client_db.id_to_info[client_id]->query_to_send = std::make_unique<distribicom::WorkerTaskPart>();
             client_db.id_to_info[client_id]->query_to_send->mutable_matrixpart()->set_row(0);
             client_db.id_to_info[client_id]->query_to_send->mutable_matrixpart()->set_col(client_id);
             
             if (!query.empty() && !query[0].empty()) {
                 std::stringstream q_ss;
                 query[0][0].save(q_ss, seal::compr_mode_type::none);
                 client_db.id_to_info[client_id]->query_to_send->mutable_matrixpart()->mutable_ctx()->set_data(q_ss.str());
             }
             // --- END SERIALIZATION ---
             
             // Prepare Partial Answer container
             // We need to know dimensions. Usually Manager handles this on 'register_client'.
             // Here we manually inject.
             int rows = db_rows; // app_configs.configs().db_rows();
             int cols = db_cols; // app_configs.configs().db_cols();
             
             // Fix: partial_answer is matrix<Plaintext>, not Ciphertext
             client_db.id_to_info[client_id]->partial_answer = std::make_shared<math_utils::matrix<seal::Plaintext>>(rows, cols);
        }
        
        // Initialize Epoch (Distribute Responsibility based on current ClientDB)
        std::cout << "[SharedMem] Starting Epoch/Round " << q << "..." << std::endl;
        auth_server->start_epoch(q, true);

        auto t_start = std::chrono::high_resolution_clock::now();

        // 1. Distribute Work (Send Queries -> Workers -> Workers Compute -> SubmitResult -> Manager)
        std::cout << "[SharedMem] Manager Distributing Work..." << std::endl;
        auto ledger = auth_server->distribute_work(q); // q is round

        // 2. Wait for Workers
        std::cout << "[SharedMem] Waiting for Workers..." << std::endl;
        auth_server->wait_for_workers(num_workers_expected); // Wait on semaphore
        
        // 3. Run Step 2 (Final Aggregation)
        std::cout << "[SharedMem] Aggregating Results..." << std::endl;
        auth_server->run_step_2(ledger);

        auto t_end = std::chrono::high_resolution_clock::now();
        double last_network_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_start).count();
        total_network_time += last_network_ms;
        measured_rounds++;
        
        // 4. Decode Reply
        std::cout << "[SharedMem] Decoding Reply..." << std::endl;
        auto& final_ans = client_db.id_to_info[client_id]->final_answer;
        if (!final_ans || final_ans->data.empty()) {
             std::cerr << "[SharedMem] Error: No final answer produced!" << std::endl;
             std::cout << "Main: query " << q << " failed (no result)!" << std::endl;
             continue;
        }

        // Convert matrix<Ciphertext> back to PirReply (vector<Ciphertext>)
        PirReply reply;
        reply.resize(d); // dimension
        // In SealPIR, reply is specific. DPIR returns a single CT usually for 1 item?
        // or a matrix. "calculate_final_answer" returns expanded result.
        // We know final_ans is 1x1 or similar depending on query expand.
        // Let's assume the result is just the ciphertexts we need.
        
        // NOTE: Standard SealPIR decode expects a vector of CTs.
        // DPIR `final_answer` is `matrix<Ciphertext>`. 
        // We push them into the reply vector.
        for(const auto& ct : final_ans->data) {
            reply.push_back(ct);
        }
        
        auto t_decode_start = std::chrono::high_resolution_clock::now();
        vector<uint8_t> elems = client.decode_reply(reply, offset);
        auto t_decode_end = std::chrono::high_resolution_clock::now();
        total_decode_time += std::chrono::duration_cast<std::chrono::milliseconds>(t_decode_end - t_decode_start).count();
        
        bool failed = false;
        auto *ptr = elems.data();
        for (uint64_t i = 0; i < size_per_item; i++) {
             if (ptr[i] != db_copy.get()[(ele_index * size_per_item) + i]) {
                 failed = true;
             }
        }
        if (failed) {
            std::cout << "Main: query " << q << " failed (data mismatch)!" << std::endl;
        } else {
            success_count++;
            std::cout << "Main: query " << q << " Success!" << std::endl;
        }
        
        // Removed start_epoch from end of loop
    }
    
    std::cout << "Success rate: " << success_count << "/" << num_queries << std::endl;
    
    std::cout << "==================================================" << std::endl;
    std::cout << "             AVERAGE TIMING RESULTS               " << std::endl;
    std::cout << "==================================================" << std::endl;

    if (measured_rounds > 0) {
         double avg_gen_time = total_gen_time / measured_rounds;
         double avg_server_time = total_network_time / measured_rounds;
         double avg_decode_time = total_decode_time / measured_rounds;
         double avg_total = avg_gen_time + avg_server_time + avg_decode_time;

         std::cout << "[SealPIR] Client Query Gen:     " << avg_gen_time << " ms" << std::endl;
         std::cout << "[SealPIR] Server Processing:    " << avg_server_time << " ms" << std::endl;
         std::cout << "[SealPIR] Client Decode:        " << avg_decode_time << " ms" << std::endl;
         std::cout << "--------------------------------------------------" << std::endl;
         std::cout << "[SealPIR] Total Latency:        " << avg_total << " ms" << std::endl;
    } else {
         std::cout << "No successful rounds measured." << std::endl;
    }
    std::cout << "==================================================" << std::endl;

    std::cout << "[SharedMem] Shutting down..." << std::endl;
    
    for(auto& w : workers) {
        w->close();
    }
    workers.clear();

    // Close manager before full shutdown
    auth_server->close();
    auth_server->Shutdown();
    
    std::cout << "[SharedMem] Finished." << std::endl;
    return 0;
}
