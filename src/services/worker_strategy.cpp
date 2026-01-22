#include "worker_strategy.hpp"
#include "utils.hpp"

namespace services::work_strategy {
    WorkerStrategy::WorkerStrategy(const seal::EncryptionParameters &enc_params,
                                   std::shared_ptr<IResponder> responder, double malicious_probability) noexcept
            : gkeys(), responder_(std::move(responder)), malicious_probability(malicious_probability) {

        pool = std::make_shared<concurrency::threadpool>();

        query_expander = math_utils::QueryExpander::Create(
                enc_params,
                pool
        );

        matops = math_utils::MatrixOperations::Create(
                math_utils::EvaluatorWrapper::Create(enc_params),
                pool
        );
    }

    void RowMultiplicationStrategy::expand_queries(WorkerServiceTask &task) {
        const std::string fname = "RowMultiplicationStrategy::expand_queries: ";
        if (!task.ptx_rows.empty()) {
            // Update Cache
            // Update Cache (Move is efficient)
            // std::cout << fname << "Updating DB Cache..." << std::endl;
            cached_db_rows = std::move(task.ptx_rows);
        } else if (!cached_db_rows.empty()) {
            // std::cout << fname << "Using Cached DB." << std::endl;
        } else {
            // No Data and No Cache. This is fine if we are just receiving Queries.
            std::cout << fname << "No DB data yet. Processing Queries..." << std::endl;
        }

        if (task.ctx_cols.empty()) {
            return;
        }
        auto exp_time = utils::time_it([&]() {

            // define the map back from col to query index.
            std::map<int, int> query_pos_to_col;
            auto col = -1;
            for (auto &key_val: task.ctx_cols) {
                col_to_query_index[++col] = key_val.first;
                query_pos_to_col[key_val.first] = col;
            }
            std::cout << "col_to_query_index: " << std::endl;
            for (auto &key_val: col_to_query_index) {
                std::cout << key_val.first << " -> " << key_val.second << std::endl;
            }


            std::cout << "expanding:" << task.ctx_cols.size() << " queries" << std::endl;
            parallel_expansions_into_query_mat(task, query_pos_to_col);

            gkeys.clear();

        });
        std::cout << "worker::expansion time: " << exp_time << " ms" << std::endl;

    }

    void
    RowMultiplicationStrategy::parallel_expansions_into_query_mat(WorkerServiceTask &task,
                                                                  std::map<int, int> &query_pos_to_col) {
        auto expanded_size = task.row_size;

        std::lock_guard lock(mu);
        // prepare the query matrix.
        // query_mat.resize(task.row_size, task.ctx_cols.size());
        query_mat.resize(task.row_size, task.ctx_cols.size());

        auto latch = std::make_shared<concurrency::safelatch>(int(task.ctx_cols.size()));
        for (auto &pair: task.ctx_cols) {
            auto q_pos = pair.first;

            if (gkeys.find(q_pos) == gkeys.end()) {
                throw std::runtime_error("galois keys not found for query position " + std::to_string(q_pos));
            }

            pool->submit(
                    std::move(concurrency::Task{
                            .f = [&, q_pos]() {
                                try {
                                    auto expanded = query_expander->expand_query(
                                            task.ctx_cols[q_pos],
                                            expanded_size,
                                            gkeys[q_pos]
                                    );

                                    auto col = query_pos_to_col.at(q_pos);
                                    for (std::uint64_t row = 0; row < expanded_size; ++row) {
                                        query_mat(row, col) = std::move(expanded[row]);
                                    }

                                    expanded.clear();
                                } catch (const std::exception& e) {
                                    std::cerr << "worker::expand_query exception: " << e.what() << std::endl;
                                    throw; // Propagate to threadpool to handle? Threadpool catches it too.
                                }
                                // latch provided to wg will be counted down automatically
                            },
                            .wg = latch,
                            .name = "worker::expand_query",
                    })
            );

        }
        latch->wait();
        task.ctx_cols.clear();

        matops->to_ntt(query_mat.data);
    }

    math_utils::matrix<seal::Ciphertext> RowMultiplicationStrategy::multiply_rows(WorkerServiceTask &task) {
        auto start = std::chrono::high_resolution_clock::now();

        // i have queries, which are columns to multiply with the rows.
        // i set the task's ptxs in a matrix of the correct size.
        // YTH: Use Cached DB
        auto mat_size = int(cached_db_rows.size());
        math_utils::matrix<seal::Plaintext> ptxs(mat_size, task.row_size);

        // moving from a map of rows to a matrix.
        auto row = -1;
        // YTH: Use Cached DB
        for (auto &pair: cached_db_rows) {
            row += 1;
            auto& ptx_row = pair.second; // Use reference, don't move from cache!
            for (std::uint64_t col = 0; col < ptx_row.size(); ++col) {
                // Copying is necessary because we want to keep the cache!
                // Seal Plaintext copy is distinct. 
                // But wait, `ptxs(row, col) = std::move(ptx_row[col])` was original.
                // We CANNOT move if we want to cache. We must COPY.
                ptxs(row, col) = ptx_row[col]; 
            }
        }

        math_utils::matrix<seal::Ciphertext> result;
        matops->multiply(ptxs, query_mat, result);

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "worker::multiplication time: " << duration.count() << "ms" << std::endl;
        this->last_computation_time_ms = duration.count();

        return result;
    }

    void random_wait_time() {
        std::random_device random;
        std::uint64_t wait_time = random() % 1500;

        //sleep for wait_time milliseconds
        std::this_thread::sleep_for(std::chrono::milliseconds(wait_time));
    }

    void
    RowMultiplicationStrategy::send_response(
            const WorkerServiceTask &task,
            math_utils::matrix<seal::Ciphertext> &computed
    ) {
        auto sending_time = utils::time_it([&]() {

            std::map<int, int> row_to_index;
            auto row = -1;
            // YTH: Use Cached DB for index mapping
            for (auto &p: cached_db_rows) {
                row_to_index[++row] = p.first;
            }

            // before sending the response - ensure we send it in reg-form to compress the data a bit more.
            matops->from_ntt(computed.data);

            // Send parts via callback
            // Note: In ZMQ/ISender model, we don't have a "stream" object to finish.
            // We just fire calls.
            
            // NOTE: We could parallelize this somewhat, but calling callback (which writes to socket) 
            // from multiple threads might need synchronization in the callback impl.
            // ZMQ Dealer is thread-safe ONLY IF we migrate use or lock. 
            // My ZMQ implementation uses a mutex OR runs on main thread?
            // "ZmqWorkStream::add_task_to_write" uses "ZmqManager::send_to_worker" which uses "queue_mtx_".
            // So it IS thread safe.
            
            // However, we want to batch or just loop?
            // Let's loop for simplicity first.
            
            auto latch = std::make_shared<concurrency::safelatch>(computed.rows * computed.cols);

            for (uint64_t i = 0; i < computed.rows; ++i) {
                for (uint64_t j = 0; j < computed.cols; ++j) {
                     pool->submit({
                         .f = [&, i, j]() {
                            distribicom::MatrixPart part;
                            part.set_row(row_to_index.at(int(i)));
                            part.set_col(col_to_query_index.at(int(j)));
                            part.mutable_ctx()->set_data(mrshl->marshal_seal_object(computed(i, j)));
                            
                            // Send compute time only on the first part or all? 
                            // Manager accumulates so we should send it only ONCE per task, or divide it.
                            // Better: Send it on the FIRST part of the batch for this worker.
                            // Since we parallelize, easy way is to check i==0 && j==0.
                            if (i == 0 && j == 0) {
                                part.set_compute_time(this->last_computation_time_ms);
                            }
                            
                            // Invoke Responder
                            if (responder_) {
                                responder_->send_part(part);
                            }
                         },
                         .wg = latch,
                         .name = "worker::send_response_task"
                     });
                }
            }
            latch->wait();
            
            // random_wait_time(); // YTH: Removed artificial delay for performance benchmarking
            
            if (responder_) {
                 responder_->finish_response();
            }
        });
        std::cout << "worker::sending time: " << sending_time << "ms" << std::endl;
    }
}
