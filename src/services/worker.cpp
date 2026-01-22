#include "services/worker.hpp"
#include "services/manager.hpp" // For SubmitResult
#include "utils.hpp"
#include "constants.hpp"
#include "distribicom.pb.h"
#include <random> 
#include <iostream>

namespace services {

    // Helper Class
    class WorkerResponder : public work_strategy::IResponder {
        Worker& worker_;
    public:
        explicit WorkerResponder(Worker& w) : worker_(w) {}
        void start_response(int round, int epoch, double compute_time) override {
            worker_.start_response(round, epoch, compute_time);
        }
        void send_part(const distribicom::MatrixPart& part) override {
            worker_.send_part(part);
        }
        void finish_response() override {
            worker_.finish_response();
        }
    };

    Worker::Worker(distribicom::AppConfigs &&wcnfgs)
        : symmetric_secret_key(), appcnfgs(std::move(wcnfgs)), chan(), mrshl(), inbox_(std::make_shared<concurrency::Channel<std::shared_ptr<distribicom::WorkerTaskPart>>>()) {

        auto seed = std::random_device()();
        std::mt19937 gen(seed);
        std::uniform_int_distribution<unsigned short> dist(0, 255);
        symmetric_secret_key.resize(services::constants::sym_key_size); // 32 bytes
        for (size_t i = 0; i < services::constants::sym_key_size; i++) {
            symmetric_secret_key[i] = (std::byte) dist(gen);
        }
        
        // Generate simulated ID
        credential_id_ = "WORKER_" + std::to_string(std::abs((int)gen())); // simple ID
        
        // Inspect configs
        if (appcnfgs.configs().scheme() != "bgv") {
            throw std::invalid_argument("currently not supporting any scheme oother than bgv.");
        }
        auto row_size = appcnfgs.configs().db_cols();
        if (row_size <= 0 || row_size > 10 * 1000) {
            throw std::invalid_argument("row size invalid");
        }

        seal::EncryptionParameters enc_params = utils::setup_enc_params(appcnfgs);
        mrshl = std::make_shared<marshal::Marshaller>(enc_params);

        // Create Strategy with Responder
        auto responder = std::make_shared<WorkerResponder>(*this);
        
        strategy = std::make_shared<work_strategy::RowMultiplicationStrategy>(
            enc_params, 
            mrshl, 
            responder, 
            utils::byte_vec_to_64base_string(symmetric_secret_key), 
            appcnfgs.configs().malicious_probability()
        );
        
        // Start Processing Thread
        running_ = true;
        threads.emplace_back(
            [&]() {
                std::cout << "worker main thread: running" << std::endl;
                for (;;) {
                    auto task_ = chan.read();
                    if (!task_.ok || !running_) {
                        std::cout << "worker main thread: stopping execution" << std::endl;
                        break;
                    }
                    // std::cout << "worker main thread: processing task." << std::endl;
                    strategy->process_task(std::move(task_.answer));
                }
            });
    }

    void Worker::run_session() {
        // Start the Inbox Listener Thread
        threads.emplace_back([this]() {
            std::cout << "[Worker " << credential_id_ << "] Starting Inbox Listener..." << std::endl;
            
            // Initialize task structure
            WorkerServiceTask current_task;
            current_task.row_size = int(appcnfgs.configs().db_cols());

            while(running_) {
                auto msg_res = inbox_->read();
                if (!msg_res.ok) {
                    std::cout << "[Worker] Inbox closed. Stopping." << std::endl;
                    break;
                }
                auto task_part = msg_res.answer;
                
                // Process Logic similar to previous OnReadDone
                try {
                     if (task_part->has_matrixpartbatch()) {
                          for(const auto& part : task_part->matrixpartbatch().parts()) {
                              // update_current_task logic inline or separate
                              int row = part.row();
                              int col = part.col();

                              if (part.has_ptx()) {
                                  if (!current_task.ptx_rows.contains(row)) {
                                      current_task.ptx_rows[row] = std::move(std::vector<seal::Plaintext>(current_task.row_size));
                                  }
                                  current_task.ptx_rows[row][col] = std::move(mrshl->unmarshal_seal_object<seal::Plaintext>(part.ptx().data()));
                              } else if (part.has_ctx()) {
                                  if (!current_task.ctx_cols.contains(col)) {
                                      current_task.ctx_cols[col] = std::move(std::vector<seal::Ciphertext>(1));
                                  }
                                  current_task.ctx_cols[col][0] = std::move(mrshl->unmarshal_seal_object<seal::Ciphertext>(part.ctx().data()));
                              }
                          }
                      } else if (task_part->has_matrixpart()) {
                          auto& part = task_part->matrixpart();
                           int row = part.row();
                           int col = part.col();

                           if (part.has_ptx()) {
                               if (!current_task.ptx_rows.contains(row)) {
                                   current_task.ptx_rows[row] = std::move(std::vector<seal::Plaintext>(current_task.row_size));
                               }
                               current_task.ptx_rows[row][col] = std::move(mrshl->unmarshal_seal_object<seal::Plaintext>(part.ptx().data()));
                           } else if (part.has_ctx()) {
                               if (!current_task.ctx_cols.contains(col)) {
                                   current_task.ctx_cols[col] = std::move(std::vector<seal::Ciphertext>(1));
                               }
                               current_task.ctx_cols[col][0] = std::move(mrshl->unmarshal_seal_object<seal::Ciphertext>(part.ctx().data()));
                           }
                      } else if (task_part->has_md()) {
                          // std::cout << "[Worker] Received Round/Epoch info." << std::endl;
                          current_task.round = int(task_part->md().round());
                          current_task.epoch = int(task_part->md().epoch());
                      } else if (task_part->has_gkey()) {
                           // Special Case: HMAC Challenge
                           if (task_part->gkey().key_pos() == 9999) {
                                std::string nonce = task_part->gkey().keys();
                                std::cout << "[Worker] Received Challenge Nonce: " << nonce << std::endl;
                                
                                std::string hmac_payload = "HMAC_PROOF_" + nonce; 
                                
                                auto resp_part = std::make_shared<distribicom::MatrixPart>();
                                resp_part->set_row(9999);
                                resp_part->set_col(9999);
                                resp_part->mutable_ctx()->set_data(hmac_payload);
                                
                                auto mgr = manager_.lock();
                                if (mgr) {
                                    mgr->SubmitResult(credential_id_, resp_part);
                                }
                           } else {
                               strategy->store_galois_key(
                                   std::move(mrshl->unmarshal_seal_object<seal::GaloisKeys>(task_part->gkey().keys())),
                                   int(task_part->gkey().key_pos())
                               );
                           }
                      } else if (task_part->has_task_complete()) {
                          // std::cout << "[Worker] Task Complete Received." << std::endl;
                          if (!current_task.ptx_rows.empty() || !current_task.ctx_cols.empty()) {
                              chan.write(std::move(current_task));
                              current_task = WorkerServiceTask();
                              current_task.row_size = int(appcnfgs.configs().db_cols());
                          }
                      }

                } catch (const std::exception& e) {
                    std::cerr << "Worker Inbox Error: " << e.what() << std::endl;
                }
            }
        });
    }

    void Worker::start_response(int round, int epoch, double compute_time) {
        // No setup needed for shared memory, just ready to call send_part
        // We could maybe send metadata here if needed.
    }

    void Worker::send_part(const distribicom::MatrixPart& part) {
        auto mgr = manager_.lock();
        if (mgr) {
            // Copy part to shared_ptr
            auto part_ptr = std::make_shared<distribicom::MatrixPart>(part);
            mgr->SubmitResult(credential_id_, part_ptr);
        } else {
            std::cerr << "Worker disconnected from manager!" << std::endl;
        }
    }

    void Worker::finish_response() {
        // Nothing to flush
    }

    void Worker::close() {
        running_ = false;
        inbox_->close();
        chan.close();
        for (auto &t: threads) {
            if(t.joinable()) t.join();
        }
    }

    Worker::~Worker() {
        close();
    }
}
