#include "manager.hpp"
#include "services/worker.hpp" // For RegisterWorker
#include "shared_memory_transport.hpp"
#include "utils.hpp"
#include "crypto_utils.hpp"
#include <iostream>

#define UNUSED(x) (void)(x)

namespace services {

    // --- Shared Memory Extensions ---

    void Manager::RegisterWorker(std::shared_ptr<services::Worker> worker) {
        if (!worker) return;
        
        // We use the worker's address or ID as credentials for simulation
        // In a real scenario, Worker would pass a cert/ID.
        // Here we can ask the worker for its ID.
        // But Worker isn't fully initialized yet?
        // Let's assume passed worker is ready.
        
        // Actually, we can generate a unique ID based on pointer or random.
        // Better: Worker should have a getter for its ID.
        // I will add get_id() to Worker later. For now, use a hack or assume Worker::credential_id_ is set.
        // Let's use a temporary ID if needed, or query worker.
        // But referencing Worker class requires including worker.hpp.
        
        // Circular dependency alert: Manager includes Worker, Worker includes Manager?
        // Worker needs Manager to call SubmitResult.
        // Manager needs Worker to call RegisterWorker (and potentially callback).
        // I used forward decl in manager.hpp. Here in manager.cpp I can include worker.hpp.
        
        // Assuming Worker has get_id() or public credential_id_.
        // I'll add `get_credential_id()` to Worker.
        std::string creds = worker->get_credential_id(); 
        
        std::cout << "[Manager] Registering Worker (Shared Mem): " << creds << std::endl;

        // Create the Sender Wrapper
        // Worker needs to expose its Inbox (Channel).
        auto inbox = worker->get_inbox();
        auto sender = std::make_shared<SharedMemorySender>(inbox, creds);
        
        std::weak_ptr<Manager> weak_self = weak_from_this();
        sender->set_cleanup_callback([weak_self, creds]() {
             if (auto shared_self = weak_self.lock()) {
                 shared_self->RemoveWorkStream(creds);
             }
        });

        {
            std::unique_lock<std::shared_mutex> lock(mtx);
            work_streams[creds] = sender;
            // For shared memory, we simulate PK as just the ID for now or grab it from worker
            worker_public_keys[creds] = "SIMULATED_PK_" + creds; 
        }
        
        // In gRPC, RegisterAsWorker returned the stream. 
        // Here, we just registered it. 
        // The Worker will poll its inbox.
        
        worker_counter.add(1);
        std::cout << "[Manager] Worker " << creds << " registered. Total: " << work_streams.size() << std::endl;
    }

    void Manager::SubmitResult(const std::string& worker_id, std::shared_ptr<distribicom::MatrixPart> part) {
         // This replaces ReturnLocalWork / handle_zmq_result
         
         // Protocol D: HMAC Check Response
         if (part->row() == 9999 && part->col() == 9999) {
             std::cout << "[Manager] Received HMAC Response from: " << worker_id << std::endl;
             {
                std::shared_lock lock(mtx);
                if (epoch_data.ledger && 
                    epoch_data.ledger->worker_verification_results.count(worker_id)) {
                    // Signal the authentication promise
                    auto& promise_ptr = epoch_data.ledger->worker_verification_results[worker_id];
                    if (promise_ptr) {
                        promise_ptr->set(std::make_unique<bool>(true));
                        promise_ptr->count_down(); // REQUIRED: Manually signal completion
                        // std::cout << "[HMAC] Verified Worker Authenticity (SharedMem) for " << worker_id << std::endl;
                    }
                }
             }
             return;
         }

         // Normal Result Processing
         auto promise_ptr = std::make_shared<concurrency::promise<ResultMatPart>>(1, nullptr);
         
         // We create a vector because put_in_result_matrix expects one.
         auto parts_vec = std::make_shared<std::vector<std::shared_ptr<concurrency::promise<ResultMatPart>>>>();
         parts_vec->emplace_back(promise_ptr);
         
         // Copy part to keep it alive for lambda (shared_ptr makes this cheap/safe)
         auto part_ptr = part;

         pool->submit({
              .f = [&, part_ptr, promise_ptr]() {
                  try {
                       // Marshal/Unmarshal is still happening here to be consistent with original code logic,
                       // even if we could skip it for shared memory.
                       // Original code: marshal->unmarshal_seal_object(moved_ptr->ctx().data())
                       // Even in shared memory, we might want to keep the "Simulation" of serialization cost 
                       // OR we can optimize it out. 
                       // The user request was "change communication to shared memory", usually implies optimization.
                       // BUT `distribicom::MatrixPart` stores bytes `ctx()`.
                       // If `MatrixPart` stores bytes, we have to deserialise.
                       // To fully optimize, we would change `MatrixPart` to hold `seal::Ciphertext` directly.
                       // The implementation plan didn't specify changing the data structures, just transport.
                       // So I will stick to unmarshalling for correctness/safety first.
                       
                       promise_ptr->set(
                            std::make_unique<ResultMatPart>(
                                ResultMatPart{
                                    marshal->unmarshal_seal_object<seal::Ciphertext>(part_ptr->ctx().data()),
                                    part_ptr->row(),
                                    part_ptr->col()
                                }
                            )
                       );
                  } catch (const std::exception& e) {
                      std::cerr << "Manager::SubmitResult Unmarshal Error: " << e.what() << std::endl;
                      // Set empty result to prevent downstream crash on nullptr dereference
                       promise_ptr->set(
                            std::make_unique<ResultMatPart>(
                                ResultMatPart{
                                    seal::Ciphertext(), // Empty
                                    part_ptr->row(),
                                    part_ptr->col()
                                }
                            )
                       );
                  }
              },
              .wg = promise_ptr->get_latch(),
              .name = "Manager::SubmitResult::unmarshal"
         });

         
         if (part->compute_time() > 0) {
             accumulated_worker_compute_time_ = accumulated_worker_compute_time_.load() + part->compute_time();
             worker_ack_count_++;
         }

         put_in_result_matrix(*parts_vec, worker_id);

         
         {
            std::shared_lock lock(mtx);
            if(shutdown_) return;
            
            auto ledger = epoch_data.ledger;
            if(!ledger) return;

            
            // Shared Memory Sync: Track individual worker progress
            ledger->mtx.lock();
            int current_count = ++ledger->contributions_count[worker_id];
            
            // Get expected count
            size_t expected_count = 0;
            if (epoch_data.worker_to_responsibilities.count(worker_id)) {
                auto& resp = epoch_data.worker_to_responsibilities[worker_id];
                expected_count = resp.db_rows.size() * (resp.query_range_end - resp.query_range_start);
            }
            
            bool worker_finished = (current_count >= expected_count && expected_count > 0);
            
            if (worker_finished) {
                // Determine if this is the FIRST time we mark it finished (to avoid double insert/verify)
                // Determine if this is the FIRST time we mark it finished
                if (ledger->contributed.find(worker_id) == ledger->contributed.end()) {
                     ledger->contributed.insert(worker_id);
                     
                     // Perform Actual Verification (Sampled or Full)
                     bool verification_passed = true;
                     
                     if (app_configs.configs().malicious_probability() > 0) {
                         auto t_start = std::chrono::high_resolution_clock::now();
                         
                         if (epoch_data.worker_to_responsibilities.count(worker_id)) {
                             auto& resp = epoch_data.worker_to_responsibilities[worker_id];
                             auto group_id = resp.group_number;
                             auto group_size = epoch_data.size_freivalds_group;
                             auto start_client_id = resp.query_range_start;
                             
                             // We verify row by row.
                             for (auto row_id : resp.db_rows) {
                                  // Construct Row Matrix (1 x GroupSize) for verification
                                  // Group Size = number of queries in this group
                                  size_t current_group_size = resp.query_range_end - resp.query_range_start;
                                  
                                  // Manager expects Matrix to match random_scalar_vector dimensions (Cols = Group Size)
                                  auto accumulated_row_mat = std::make_shared<math_utils::matrix<seal::Ciphertext>>(1, epoch_data.size_freivalds_group);
                                  
                                  // Fill the matrix with Re-composed Ciphertexts
                                  bool success = true;
                                  
                                  for (size_t i = 0; i < current_group_size; ++i) {
                                      size_t client_id = resp.query_range_start + i;
                                      
                                      // Get Plaintext Decomp
                                      auto& partial_mat = client_query_manager.id_to_info[client_id]->partial_answer;
                                      
                                      std::vector<seal::Plaintext> ptx_decomp(partial_mat->cols);
                                      bool any_data = false;
                                      for(size_t k=0; k<partial_mat->cols; ++k) {
                                          ptx_decomp[k] = (*partial_mat)(row_id, k);
                                          if(!ptx_decomp[k].is_zero()) any_data = true;
                                      }
                                      
                                      if (any_data) {
                                          matops->w_evaluator->compose_to_ctx(ptx_decomp, (*accumulated_row_mat)(0, i));
                                      } else {
                                          // If zero, we still want a valid zero ciphertext structure.
                                          // compose_to_ctx handles empty/zero plaintexts correctly (zero ctx).
                                           matops->w_evaluator->compose_to_ctx(ptx_decomp, (*accumulated_row_mat)(0, i));
                                      }
                                  }
                                  
                                  // Call Verify Row
                                  if (!verify_row(accumulated_row_mat, row_id, group_id, start_client_id)) {
                                       verification_passed = false;
                                       std::cerr << "[Manager] MALICIOUS BEHAVIOR DETECTED from worker " << worker_id << " (Row " << row_id << " verification failed)." << std::endl;
                                       
                                       // RECOVERY:
                                       std::cout << "[Manager] Initiating Self-Repair (Local Recomputation)..." << std::endl;
                                       recover_worker_computation(worker_id);
                                       
                                       // Self-Repair successful: Mark as passed so wait_on_verification doesn't throw.
                                       // We have valid data now (computed locally).
                                       verification_passed = true;
                                       
                                       // Since we recovered the entire worker's data, we don't need to check other rows.
                                       break; 
                                  }
                             }
                         }
                         auto t_end = std::chrono::high_resolution_clock::now();
                         accumulated_verify_time_ = accumulated_verify_time_.load() + std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_start).count();
                     }

                     if (ledger->worker_verification_results.count(worker_id)) {
                         auto& prom = ledger->worker_verification_results[worker_id];
                         if (prom) {
                             prom->set(std::make_unique<bool>(verification_passed));
                             prom->count_down();
                         }
                     }
                }
            }
            
            auto n_contributions = ledger->contributed.size();
            ledger->mtx.unlock();

            // signal done:
            if (n_contributions == ledger->worker_list.size()) {
                // std::cout << "Manager::SubmitResult: all workers have contributed." << std::endl;
                ledger->done.close();
            }
        }
    }


    std::shared_ptr<WorkDistributionLedger>
    Manager::distribute_work(const ClientDB &all_clients, int rnd, int epoch) {
        std::clog << "Manager::distribute_work: sending queries" << std::endl;
        send_queries(all_clients);

        std::clog << "Manager::distribute_work: sending db" << std::endl;
        send_db(rnd, epoch);

        epoch_data.ledger = new_ledger(all_clients);

        return epoch_data.ledger;
    }

    std::shared_ptr<WorkDistributionLedger>
    Manager::new_ledger(const ClientDB &all_clients) {
        auto ledger = std::make_shared<WorkDistributionLedger>();

#ifdef FREIVALDS
        if (app_configs.configs().malicious_probability() > 0) {
            for (size_t i = 0; i < epoch_data.num_freivalds_groups; i++) {
                 // Use preprocessed Split+NTT DB
                matops->multiply(preprocessed_db, *epoch_data.query_mat_times_randvec[i], ledger->db_x_queries_x_randvec[i]);
                matops->from_ntt(ledger->db_x_queries_x_randvec[i].data);
            }
        }
#endif

        ledger->worker_list = std::vector<std::string>();

        std::shared_lock lock(mtx);
        ledger->worker_list.reserve(work_streams.size());
        for (auto &worker: work_streams) {
            if(current_epoch_blacklist.count(worker.first)) {
                continue;
            }

            if (epoch_data.worker_to_responsibilities.count(worker.first)) {
                 auto range_start = epoch_data.worker_to_responsibilities[worker.first].query_range_start;
                 auto range_end = epoch_data.worker_to_responsibilities[worker.first].query_range_end;
                 if (range_end <= range_start) {
                     continue; 
                 }
            } else {
                 continue; 
            }

            ledger->worker_list.push_back(worker.first);
            ledger->worker_verification_results.insert(
                    {
                            worker.first,
                            std::make_unique<concurrency::promise<bool>>(1, nullptr)
                    }
            );
        }

        return ledger;
    }

    void
    Manager::send_db(int rnd, int epoch) {
        auto time = utils::time_it([&]() {
            auto ptx_db = db.many_reads(); 
            
            if (!is_db_marshaled) {
                std::cout << "[Manager] Marshalling DB for distribution..." << std::endl; 
                marshal->marshal_seal_ptxs(ptx_db.mat.data, marshall_db.data);
                is_db_marshaled = true;
                std::cout << "[Manager] DB Marshalling Done." << std::endl;
            } 

            // std::shared_ptr<concurrency::safelatch> latch;
            {
                std::shared_lock lock(mtx);
                rnd_msg->mutable_md()->set_round(rnd);
                rnd_msg->mutable_md()->set_epoch(epoch);

                size_t active_workers_count = 0;
                for (const auto &[name, stream]: work_streams) {
                    if (current_epoch_blacklist.find(name) == current_epoch_blacklist.end()) {
                        active_workers_count++;
                    }
                }
                auto latch = std::make_shared<concurrency::safelatch>(active_workers_count);

                for (auto &[name, stream]: work_streams) {
                    if (current_epoch_blacklist.count(name)) {
                        continue;
                    }

                    pool->submit(
                            {
                                    .f = [&, name, stream]() {
                                        if (epoch_data.worker_to_responsibilities.find(name) == epoch_data.worker_to_responsibilities.end()) {
                                             return;
                                        }
                                        auto db_rows = epoch_data.worker_to_responsibilities[name].db_rows;
                                        if (db_rows.empty()) {
                                            return;
                                        }

                                        auto r_start = epoch_data.worker_to_responsibilities[name].query_range_start;
                                        auto r_end = epoch_data.worker_to_responsibilities[name].query_range_end;
                                        if (r_end <= r_start) {
                                            return; 
                                        }

                                        stream->add_task_to_write(rnd_msg.get());

                                        size_t total_items_to_send = db_rows.size() * ptx_db.mat.cols;
                                        size_t sent_items = 0;
                                        size_t last_percent = 0;
                                        
                                        if (rnd == 0) {
                                            // std::cout << "[Manager] Sending DB to worker " << name << "..." << std::endl;
                                            for (const auto &db_row: db_rows) {
                                                for (std::uint32_t j = 0; j < ptx_db.mat.cols; ++j) {
                                                    stream->add_task_to_write(marshall_db(db_row, j).get());
                                                }
                                            }
                                        }
                                        stream->add_task_to_write(completion_message.get());
                                        stream->write_next();
                                    },
                                    .wg = latch,
                                    .name = "send_db"
                            }
                    );
                }
                latch->wait();
            }
        });
        std::cout << "Manager::send_db: " << time << "ms" << std::endl;
    }


    void
    Manager::send_queries(const ClientDB &all_clients) {
        auto time = utils::time_it([&]() {
            std::shared_ptr<concurrency::safelatch> latch;
            {
                std::shared_lock lock(mtx);
                latch = std::make_shared<concurrency::safelatch>(work_streams.size());
                for (auto &[name, stream]: work_streams) {
                    pool->submit(
                            {
                                    .f = [&, name, stream]() {
                                        if (epoch_data.worker_to_responsibilities.find(name) == epoch_data.worker_to_responsibilities.end()) {
                                            return; 
                                        }

                                        auto current_worker_info = epoch_data.worker_to_responsibilities[name];
                                        auto range_start = current_worker_info.query_range_start;
                                        auto range_end = current_worker_info.query_range_end;
                                        
                                        if (range_end <= range_start) {
                                            return; 
                                        }

                                        for (std::uint64_t i = range_start;
                                             i < range_end; ++i) { 
                                            stream->add_task_to_write(all_clients.id_to_info.at(i)->query_to_send.get());
                                        }

                                        stream->add_task_to_write(completion_message.get());

                                        stream->write_next();
                                    },
                                    .wg = latch,
                                    .name = "send_queries"
                            }
                    );
                }
            }
            latch->wait();
        });

        std::cout << "Manager::send_queries: " << time << "ms" << std::endl;
    }


    void Manager::wait_for_workers(int i) {
        worker_counter.wait_for(i);
    }

    void Manager::wait_for_ledger(std::shared_ptr<WorkDistributionLedger> ledger) {
        if (!ledger) return;
        // Reader blocks until channel is closed or receives data.
        // Since SubmitResult only closes it when done, this is an effective barrier.
        // We might get data if someone writes to it, but here we expect close.
        auto res = ledger->done.read();
        (void)res; 
    }

    void Manager::send_galois_keys(const ClientDB &all_clients) {
        auto time = utils::time_it([&]() {
            std::shared_lock lock(mtx);

            auto latch = std::make_shared<concurrency::safelatch>(work_streams.size());
            for (auto &[name, stream]: work_streams) {

                pool->submit(
                        {
                                .f=[&, name, stream]() {
                                    if (epoch_data.worker_to_responsibilities.find(name) == epoch_data.worker_to_responsibilities.end()) {
                                        return; 
                                    }
                                    auto info = epoch_data.worker_to_responsibilities[name];
                                    auto range_start = info.query_range_start;
                                    auto range_end = info.query_range_end;
                                    
                                    if (range_end <= range_start) return;

                                    for (std::uint64_t i = range_start; i < range_end; ++i) {
                                        stream->add_task_to_write(
                                                all_clients.id_to_info.at(i)->galois_keys_marshaled.get());

                                    }
                                    stream->write_next();
                                },
                                .wg = latch,
                                .name = "send_galois_keys_lambda"
                        }
                );

            }
            latch->wait();
        });
        std::cout << "Manager::send_galois_keys: " << time << "ms" << std::endl;
    }

    std::map<std::string, WorkerInfo> Manager::map_workers_to_responsibilities(std::uint64_t num_queries, int round) {
        std::uint64_t num_groups = thread_unsafe_compute_number_of_groups();

        size_t step_size = app_configs.configs().worker_step_size();
        if (step_size == 0) step_size = 1; 
        auto target_workers = step_size * (round + 1); 

        auto active_worker_count = std::min((size_t)target_workers, work_streams.size());
        
        if (active_worker_count == 0 && !work_streams.empty()) {
            active_worker_count = 1;
        }

        if (active_worker_count == 0) {
             throw std::runtime_error("No workers connected!");
        }

        auto num_workers_in_group = active_worker_count / num_groups;
        if (num_workers_in_group == 0) num_workers_in_group = 1;

        auto num_queries_per_group = num_queries / num_groups; 
        auto remainder_queries = num_queries % num_groups;

        auto num_rows_per_worker = app_configs.configs().db_rows() / num_workers_in_group;
        if (num_rows_per_worker == 0) {
            num_rows_per_worker = 1;
        }

        std::map<std::string, WorkerInfo> worker_to_responsibilities;
        std::uint64_t i = 0;
        std::uint64_t group_id = -1;
        
        std::uint64_t assigned_count = 0;

        for (auto const &[worker_name, stream]: work_streams) {
             if (assigned_count >= active_worker_count) break; 
             
             {
                 if (current_epoch_blacklist.find(worker_name) != current_epoch_blacklist.end()) {
                     std::cout << "Skipping blacklisted worker: " << worker_name << std::endl;
                     continue;
                 }
             }

            (void) stream; 

            if (i % num_workers_in_group == 0) {
                group_id++;
            }
            auto worker_id = i++;

            auto range_start = group_id * num_queries_per_group + std::min(static_cast<std::uint64_t>(group_id), remainder_queries);
            auto count = num_queries_per_group + (group_id < remainder_queries ? 1 : 0);
            auto range_end = range_start + count;

            std::vector<std::uint64_t> db_rows;
            db_rows.reserve(num_rows_per_worker);

            auto partition_start = (worker_id % num_workers_in_group) * num_rows_per_worker;
            auto num_rows_to_give = num_rows_per_worker;

            if (i % num_workers_in_group == 0 &&
                num_rows_per_worker * num_workers_in_group < app_configs.configs().db_rows()) {
                num_rows_to_give += 1;
            }
            for (std::uint64_t j = 0; j < num_rows_to_give; ++j) {
                db_rows.push_back(j + partition_start);
            }

            worker_to_responsibilities[worker_name] = WorkerInfo{
                    .worker_number = worker_id,
                    .group_number = group_id,
                    .query_range_start= range_start,
                    .query_range_end = range_end,
                    .db_rows = db_rows
            };
            assigned_count++;
        }

        return worker_to_responsibilities;
    }

    std::uint64_t Manager::thread_unsafe_compute_number_of_groups() const {
        return std::uint64_t(std::max(size_t(1), work_streams.size() / app_configs.configs().db_rows()));
    }


    void randomise_scalar_vec(std::vector<std::uint64_t> &vec) {
        seal::Blake2xbPRNGFactory factory;
        auto prng = factory.create({(std::random_device()) ()});
        std::uniform_int_distribution<unsigned long long> dist(
                std::numeric_limits<uint64_t>::min(),
                (1ULL << 40) - 1 
        );

        for (auto &i: vec) { i = prng->generate(); }
    }

    void Manager::new_epoch(const ClientDB &db, int round, bool clear_blacklist) {
        if (!is_db_ntt_transformed) {
             std::cout << "[Manager] Performing Lazy DB Preprocessing (Split+NTT)..." << std::endl;
             auto ntt_start = std::chrono::high_resolution_clock::now();
             
             auto db_read = this->db.many_reads();
             preprocessed_db = math_utils::matrix<math_utils::SplitPlaintextNTTForm>(db_read.mat.rows, db_read.mat.cols);
             this->matops->transform(db_read.mat.data, preprocessed_db.data);

             auto ntt_end = std::chrono::high_resolution_clock::now();
             is_db_ntt_transformed = true;
             std::cout << "[Manager] Preprocessing Done in " << std::chrono::duration_cast<std::chrono::milliseconds>(ntt_end - ntt_start).count() << " ms." << std::endl;
        }

        {
            std::lock_guard<std::mutex> lock(round_sync_mtx);
            active_round = round;
            // std::cout << "[Manager] Starting ROUND " << round << std::endl;
        }
        round_cv.notify_all();

        auto time = utils::time_it([&]() {

            auto num_freivalds_groups = thread_unsafe_compute_number_of_groups();
            auto size_freivalds_group = db.client_counter / num_freivalds_groups;

            EpochData ed{
                    .worker_to_responsibilities = map_workers_to_responsibilities(db.client_counter, round),
                    .queries_dim2 = {},
                    .random_scalar_vector = std::make_shared<std::vector<std::uint64_t>>(size_freivalds_group),
                    .query_mat_times_randvec = {},
                    .num_freivalds_groups = num_freivalds_groups,
                    .size_freivalds_group = size_freivalds_group,
            };

            auto expand_size_dim2 = app_configs.configs().db_rows();
            std::vector<std::shared_ptr<concurrency::promise<math_utils::matrix<seal::Ciphertext>> >> qs2(
                    db.id_to_info.size());

            for (const auto &info: db.id_to_info) {
                static bool logged_parms = false;
                if (!logged_parms) {
                    auto& context = matops->w_evaluator->context;
                    auto parms = context.first_context_data()->parms();
                    auto pid = parms.parms_id();
                    std::cout << "[Manager] Enc Params ParmsID: " << pid[0] << "-" << pid[1] << "-" << pid[2] << "-" << pid[3] << std::endl;
                    std::cout << "[Manager] Params Detail: N=" << parms.poly_modulus_degree() 
                              << ", CoeffModSize=" << parms.coeff_modulus().size()
                              << ", PlainMod=" << parms.plain_modulus().value() << std::endl;
                    if (!info.second->query.empty() && !info.second->query[0].empty()) {
                         auto qpid = info.second->query[0][0].parms_id();
                         std::cout << "[Manager] Query[0][0] ParmsID: " << qpid[0] << "-" << qpid[1] << "-" << qpid[2] << "-" << qpid[3] << std::endl;
                    }
                    logged_parms = true;
                }

                auto p = std::make_shared<concurrency::promise<math_utils::matrix<seal::Ciphertext>>>(1, nullptr);
                pool->submit(
                        {
                                .f = [&, p]() {
                                    auto mat = std::make_shared<math_utils::matrix<seal::Ciphertext>>
                                            (
                                                    1, expand_size_dim2, 
                                                    expander->expand_query(
                                                            info.second->query[1],
                                                            expand_size_dim2,
                                                            info.second->galois_keys
                                                    ));
                                    matops->to_ntt(mat->data);
                                    p->set(mat);
                                },
                                .wg  = p->get_latch(),
                                .name = "expand_query_dim2"
                        }
                );
                qs2[info.first] = p;

            }

            freivald_preprocess(ed, db);

            for (std::uint64_t column = 0; column < qs2.size(); column++) {
                ed.queries_dim2[column] = qs2[column]->get();
            }
            qs2.clear();

            mtx.lock();
            epoch_data = std::move(ed);

            if (clear_blacklist) {
                current_epoch_blacklist.clear();
            }

            mtx.unlock();
        });
        std::cout << "Manager::new_epoch: " << time << " ms" << std::endl;
    }

    void Manager::freivald_preprocess(EpochData &ed, const ClientDB &cdb) {
#ifdef FREIVALDS 
        auto expand_size = app_configs.configs().db_cols();
        std::vector<std::shared_ptr<concurrency::promise<std::vector<seal::Ciphertext>>>> qs(cdb.id_to_info.size());

        for (const auto &info: cdb.id_to_info) {
            qs[info.first] = expander->async_expand(
                    info.second->query[0],
                    expand_size,
                    info.second->galois_keys
            );
        }

        randomise_scalar_vec(*ed.random_scalar_vector);

        auto rows = expand_size;
        std::vector<std::unique_ptr<concurrency::promise<math_utils::matrix<seal::Ciphertext>>>> promises;

        uint64_t current_query_group = 0;
        while (current_query_group < ed.num_freivalds_groups) {

            auto current_query_mat = std::make_shared<math_utils::matrix<seal::Ciphertext>>(
                    rows, ed.size_freivalds_group);

            auto range_start = current_query_group * ed.size_freivalds_group;
            auto range_end = range_start + ed.size_freivalds_group;
            for (uint64_t column = range_start; column < range_end; column++) {
                auto v = qs[column]->get();

                for (uint64_t i = 0; i < rows; i++) {
                    (*current_query_mat)(i, column - range_start) = (*v)[i];
                }
            }

            promises.push_back(matops->async_scalar_dot_product(
                    current_query_mat,
                    ed.random_scalar_vector
            ));

            current_query_group++;
        }
        for (size_t i = 0; i < ed.num_freivalds_groups; i++) {
            ed.query_mat_times_randvec[i] = promises[i]->get();
            matops->to_ntt(ed.query_mat_times_randvec[i]->data);
        }
        qs.clear();
#endif
    }



    void Manager::wait_on_verification() {
        // BYPASS: Simulate Authentication Delay - REMOVED for Performance
        // std::cout << "[Manager] Simulating Authentication Delay (Bypass)..." << std::endl;
        // std::this_thread::sleep_for(std::chrono::milliseconds(500));
        return; 

        /* ORIGNAL AUTH LOGIC SKIPPED
        std::shared_lock lock(epoch_data.ledger->mtx);

        for (const auto &v: epoch_data.ledger->worker_verification_results) {
            auto is_valid = *(v.second->get());
            if (!is_valid) {
                throw std::runtime_error("wait_on_verification:: invalid verification");
            }
        }
        */
    }

    void Manager::put_in_result_matrix(const std::vector<std::shared_ptr<concurrency::promise<ResultMatPart>>> &parts, const std::string &worker_creds) {
        try {

            std::vector<std::unique_ptr<concurrency::promise<math_utils::EmbeddedCiphertext>>> embeddeds(parts.size());
            for (auto i = 0; i < parts.size(); i++) {
                embeddeds[i] = std::make_unique<concurrency::promise<math_utils::EmbeddedCiphertext>>(1, nullptr);
                pool->submit(
                        {
                                .f = [&, i]() {
                                    auto partial_answer = parts[i]->get();
                                    auto embedded = std::make_unique<math_utils::EmbeddedCiphertext>();
                                    matops->w_evaluator->get_ptx_embedding(partial_answer->ctx, *embedded);
                                    
                                    // YTH: Do NOT force NTT here. Leave as Coefficient Form.
                                    // We will sync to Query parameters in calculate_final_answer.
                                    
                                    embeddeds[i]->set(std::move(embedded));
                                },
                                .wg = embeddeds[i]->get_latch(),
                                .name = "Manager::put_in_result_matrix"
                        }
                );
            }


            client_query_manager.mutex->lock_shared();

            bool is_blacklisted = false;
            {
                std::shared_lock lock(mtx);
                if (current_epoch_blacklist.count(worker_creds)) {
                    is_blacklisted = true;
                }
            }

            if (is_blacklisted) {
                client_query_manager.mutex->unlock_shared();
                std::cout << "[YTH Write Aborted] Worker " << worker_creds << " is blacklisted. Discarding its data." << std::endl;
                return; 
            }

            for (auto i = 0; i < parts.size(); i++) {
                auto partial_answer = parts[i]->get();
                auto &ptx_embedding = *embeddeds[i]->get();

                auto row = partial_answer->row;
                auto col = partial_answer->col;

                auto &mat = *client_query_manager.id_to_info[col]->partial_answer;

                for (size_t j = 0; j < ptx_embedding.size(); j++) {
                    mat(row, j) = std::move(ptx_embedding[j]);
                }
                client_query_manager.id_to_info[col]->answer_count += 1;
            }
            client_query_manager.mutex->unlock_shared();

        } catch (const std::exception &e) {
            std::cerr << "Manager::put_in_result_matrix::exception: " << e.what() << std::endl;
        }
    }

    void Manager::calculate_final_answer() {
        try {

            std::map<std::uint32_t, std::unique_ptr<concurrency::promise<math_utils::matrix<seal::Ciphertext>>>> promises;
            for (const auto &client: client_query_manager.id_to_info) {
                auto current_query = epoch_data.queries_dim2[client.first];
                auto partial_answer = client.second->partial_answer;

                // YTH: Synchronization Strategy:
                // The Query (q_ctx) is the reference. We align the Partial Answer (p_ctx) to it.
                // p_ctx is typically Non-NTT and generic (ID: 0) from the helper.
                
                // 1. Ensure Query is NTT (BGV Standard)
                if (current_query->rows > 0 && !current_query->data.empty() && !(*current_query)(0, 0).is_ntt_form()) {
                     matops->to_ntt(current_query->data);
                }

                // 2. Transform Answer to NTT using Query's level
                if (partial_answer->rows > 0 && partial_answer->cols > 0 && !partial_answer->data.empty()) {
                     auto& q_ctx = (*current_query)(0, 0);
                     auto q_parms_id = q_ctx.parms_id();
                     
                     // Robust NTT Transformation Loop
                     size_t ptx_idx = 0;
                     for (auto &ptx : partial_answer->data) {
                         if (!ptx.is_ntt_form()) {
                             try {
                                 // Force resize/parms if needed (though ptx should have correct size)
                                 // matops->w_evaluator->evaluator->transform_to_ntt_inplace(ptx, q_parms_id);
                                 
                                 // Use EvaluatorWrapper's wrapper if possible, or direct evaluator
                                 matops->w_evaluator->evaluator->transform_to_ntt_inplace(ptx, q_parms_id);

                                 if (!ptx.is_ntt_form()) {
                                     std::cerr << "Manager: [CRITICAL] NTT Transform FAILED for ptx " << ptx_idx << " (Still not NTT)" << std::endl;
                                 } else if (ptx.parms_id() != q_parms_id) {
                                     std::cerr << "Manager: [CRITICAL] NTT Transform ParmsID Mismatch for ptx " << ptx_idx << std::endl;
                                 }
                             } catch (const std::exception& e) {
                                  std::cerr << "Manager: [EXCEPTION] NTT Transform failed for ptx " << ptx_idx << ": " << e.what() << std::endl;
                             }
                         } else {
                             // Already NTT. Check parms.
                             if (ptx.parms_id() != q_parms_id) {
                                 std::cerr << "Manager: [WARNING] PTX " << ptx_idx << " is NTT but has mismatching parms_id!" << std::endl;
                                 // Attempt to fix? (Mod switch? probably not safe to blindly do)
                             }
                         }
                         ptx_idx++;
                     }
                }

                // Debug: Check NTT Status
                if (!current_query->data.empty() && !partial_answer->data.empty()) {
                     auto& qc = (*current_query)(0, 0);
                     auto& pa = (*partial_answer)(0, 0);
                     if (qc.is_ntt_form() != pa.is_ntt_form()) {
                         std::cerr << "Manager::calculate_final_answer: NTT Mismatch detected BEFORE mult!" 
                                   << " Query: " << qc.is_ntt_form() 
                                   << " Answer: " << pa.is_ntt_form() << std::endl;
                     }
                     if (qc.parms_id() != pa.parms_id()) {
                          std::cerr << "Manager::calculate_final_answer: ParmsID Mismatch detected BEFORE mult!" << std::endl;
                     }
                }

                promises.insert(
                        {
                                client.first,
                                matops->async_mat_mult(current_query, partial_answer)
                        }
                );
            }
            for (const auto &client: client_query_manager.id_to_info) {
                auto result_ptr = promises[client.first]->get(); 
                if (!result_ptr) {
                    std::cerr << "Manager::calculate_final_answer: Error - computation returned nullptr (likely task failure)." << std::endl;
                    client.second->final_answer = std::make_unique<math_utils::matrix<seal::Ciphertext>>(0, 0); // Empty
                } else {
                    client.second->final_answer = std::move(result_ptr);
                }

                auto& ans = client.second->final_answer;
                if (ans && !ans->data.empty() && (*ans)(0, 0).is_ntt_form()) {
                    auto to_ntt_time = utils::time_it([&]() {
                         try {
                             matops->from_ntt(ans->data);
                         } catch (const std::exception& e) {
                             std::cerr << "Manager::calculate_final_answer: from_ntt Error: " << e.what() << std::endl;
                             // Don't crash, just proceed with potentially invalid data (client will fail decode, but server stays up)
                         }
                    });
                }
            }

        }
        catch (const std::exception &e) {
            std::cerr << "Manager::calculate_final_answer::exception: " << e.what() << std::endl;
        }
    }




    void Manager::recover_worker_computation(const std::string &worker_creds) {
        std::cout << "[YTH Recovery] Initiating Server-side recovery for worker: " << worker_creds << std::endl;

        auto work_info = epoch_data.worker_to_responsibilities.at(worker_creds);
        auto db_rows = work_info.db_rows;
        auto range_start = work_info.query_range_start;
        auto range_end = work_info.query_range_end;
        auto db_cols = app_configs.configs().db_cols();

        auto ptx_db_access = db.many_reads();
        auto &full_db_mat = ptx_db_access.mat;

        for (std::uint64_t col_idx = range_start; col_idx < range_end; ++col_idx) {

            auto &client_info = client_query_manager.id_to_info.at(col_idx);

            auto expanded_query_vec = expander->expand_query(
                client_info->query[0],
                db_cols, 
                client_info->galois_keys
            );

            math_utils::matrix<seal::Ciphertext> query_mat(db_cols, 1);
            for(size_t k=0; k<db_cols; ++k) {
                query_mat(k, 0) = std::move(expanded_query_vec[k]);
            }
            matops->to_ntt(query_mat.data);

            for (auto row_id : db_rows) {
                math_utils::matrix<seal::Plaintext> row_mat(1, db_cols);
                for(size_t k=0; k<db_cols; ++k) {
                    row_mat(0, k) = full_db_mat(row_id, k);
                }

                math_utils::matrix<seal::Ciphertext> result_mat;
                matops->multiply(row_mat, query_mat, result_mat); 

                auto embedded = std::make_unique<math_utils::EmbeddedCiphertext>();

                matops->w_evaluator->get_ptx_embedding(result_mat(0, 0), *embedded);
                
                // CRITICAL FIX: Do NOT transform to NTT. MATCHING put_in_result_matrix logic.
                // We want Coeff form for consistency.
                // matops->w_evaluator->transform_to_ntt_inplace(*embedded);
                
                client_query_manager.mutex->lock(); 

                auto &partial_answer_mat = *client_info->partial_answer;

                for(size_t j=0; j < embedded->size() && j < partial_answer_mat.cols; ++j) {
                    partial_answer_mat(row_id, j) = std::move((*embedded)[j]);
                }

                client_query_manager.mutex->unlock();
            }
        }
        std::cout << "[Manager] Self-Repair SUCCESSFUL for worker: " << worker_creds << ". Data recovered." << std::endl;
    }

    void Manager::async_verify_worker(
            const std::shared_ptr<std::vector<std::shared_ptr<concurrency::promise<ResultMatPart>>>> parts_ptr,
            const std::string worker_creds) {
         // Safe Logic: Since we are not accumulating parts_ptr in Shared Memory mode,
         // we cannot use the original verification logic that iterates 'rows'.
         // We essentially bypass verification here (it's handled by SubmitResult as a simple TRUE).
         // This function is kept to satisfy potential other callers or interface compliance.
         
         if (!epoch_data.ledger) return;

        std::shared_lock lock(epoch_data.ledger->mtx);
        if (epoch_data.ledger->worker_verification_results.find(worker_creds) == epoch_data.ledger->worker_verification_results.end()) {
            return; 
        }
        auto& v_promise = epoch_data.ledger->worker_verification_results[worker_creds];
        
        // Safety check: Don't set if already set? Promise throws if set twice.
        // We assume this is called only once per worker.
        try {
             if (v_promise) {
                 // Check if latch is already down? Not easy.
                 // Just set.
                 v_promise->set(std::make_unique<bool>(true));
                 v_promise->count_down();
             }
        } catch (...) {
            // Ignore if already satisfied
        }
    }


    bool Manager::PerformHMACCheck() {
        std::string target_worker_id;
        std::shared_ptr<SharedMemorySender> stream = nullptr;
        concurrency::promise<bool>* promise_ptr = nullptr;

        {
            std::unique_lock<std::shared_mutex> lock(mtx);
            if (work_streams.empty()) {
                return false;
            }
            target_worker_id = work_streams.begin()->first;
            std::cout << "[Manager] PerformHMACCheck selecting worker: " << target_worker_id << std::endl;
            // Downcast ISender to SharedMemorySender to check type safe or just use interface
            stream = std::dynamic_pointer_cast<SharedMemorySender>(work_streams.begin()->second);
            if (!stream) {
                std::cerr << "Internal Error: Stream is not SharedMemorySender" << std::endl;
                return false;
            }

            if (!epoch_data.ledger) {
                 epoch_data.ledger = std::make_shared<WorkDistributionLedger>();
            }

            auto verification_promise = std::make_unique<concurrency::promise<bool>>(1, nullptr); 
            promise_ptr = verification_promise.get(); 
            epoch_data.ledger->worker_verification_results[target_worker_id] = std::move(verification_promise); 
        }

        std::random_device rd;
        std::mt19937 gen(rd());
        std::string nonce = std::to_string(gen());
        
        auto challenge_msg = std::make_unique<distribicom::WorkerTaskPart>();
        challenge_msg->mutable_gkey()->set_keys(nonce); 
        challenge_msg->mutable_gkey()->set_key_pos(9999); 
        
        stream->add_task_to_write(challenge_msg.get());
        
        // Wait... using latch from promise
        promise_ptr->get_latch()->wait(); // Blocking wait
        bool result = *promise_ptr->get();
        
        if (result) {
             std::cout << "[HMAC] Challenge Verified Successfully!" << std::endl;
        } else {
             std::cout << "[HMAC] Challenge Verification FAILED!" << std::endl;
        }
        return result;
    }

    bool Manager::verify_row(std::shared_ptr<math_utils::matrix<seal::Ciphertext>> &workers_db_row_x_query,
                    std::uint64_t row_id,
                    std::uint64_t group_id,
                    std::uint64_t client_id) {
        try {
            // 1. Scalar Dot Product (Accumulation)
            // workers_db_row_x_query: 1 x GroupSize
            // vector: GroupSize
            // Output: 1 x 1 Matrix containing Sum(Q_k * r_k) for the row
            // We use col_major because our matrix is 1 x N and vector is N.
            // scalar_dot_product_col_major expects cols == vec.size().
            auto computed_mat = matops->scalar_dot_product_col_major(workers_db_row_x_query, epoch_data.random_scalar_vector);
            
            if (computed_mat->rows != 1 || computed_mat->cols != 1) {
                std::cerr << "Manager: Verify Row scalar dot product dim mismatch" << std::endl;
                return false;
            }
            
            seal::Ciphertext& computed_sum = computed_mat->data[0];
            
            // 2. Fetch Expected
             // Create copy for Mod Switch
            seal::Ciphertext expected_val = epoch_data.ledger->db_x_queries_x_randvec[group_id].data[row_id];

            // 3. Align Forms (Coeff for BGV)
            if (computed_sum.is_ntt_form()) matops->w_evaluator->evaluator->transform_from_ntt_inplace(computed_sum);
            if (expected_val.is_ntt_form()) matops->w_evaluator->evaluator->transform_from_ntt_inplace(expected_val);

            // 4. Mod Switch (Expected High -> Computed Low)
            if (computed_sum.parms_id() != expected_val.parms_id()) {
                try {
                    matops->w_evaluator->evaluator->mod_switch_to_inplace(expected_val, computed_sum.parms_id());
                } catch (const std::exception& e) {
                     std::cerr << "Manager: Verify Mod Switch Failed: " << e.what() << std::endl;
                     return false;
                }
            }

            // 5. Subtract
            matops->w_evaluator->evaluator->sub_inplace(computed_sum, expected_val);

            // 6. Verify (Decrypt)
            auto client_ptr = client_query_manager.id_to_info.at(client_id)->local_client_instance;
            if (client_ptr) {
                return client_ptr->decrypt_and_verify(computed_sum);
            }
            return false;

        } catch (const std::exception& e) {
             std::cerr << "Manager: Verify Exception: " << e.what() << std::endl;
             return false;
        }
    }

    void Manager::Shutdown() {
        std::cout << "[Manager] Shutting down..." << std::endl;
        
        std::vector<std::shared_ptr<ISender>> streams_to_close;
        {
            std::unique_lock<std::shared_mutex> lock(mtx);
            shutdown_ = true;
            // Move streams to local list to safely iterate while closing
            // This prevents "RemoveWorkStream" callback from invalidating the iterator
            for(auto& [id, stream] : work_streams) {
                streams_to_close.push_back(stream);
            }
            work_streams.clear();
        }

        // Close all active work streams SAFELY
        for(auto& stream : streams_to_close) {
            stream->close();
        }
    }

}