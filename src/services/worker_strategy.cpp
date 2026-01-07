#include "worker_strategy.hpp"
#include "utils.hpp"

namespace services::work_strategy {
    WorkerStrategy::WorkerStrategy(const seal::EncryptionParameters &enc_params,
                                   std::unique_ptr<distribicom::Manager::Stub> &&manager_conn, double malicious_probability) noexcept
            : gkeys(), manager_conn(std::move(manager_conn)), malicious_probability_(malicious_probability) {

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
        query_mat.resize(task.row_size, task.ctx_cols.size());

        concurrency::safelatch latch(int(task.ctx_cols.size()));
        for (auto &pair: task.ctx_cols) {
            auto q_pos = pair.first;

            if (gkeys.find(q_pos) == gkeys.end()) {
                throw std::runtime_error("galois keys not found for query position " + std::to_string(q_pos));
            }

            pool->submit(
                    std::move(concurrency::Task{
                            .f = [&, q_pos]() {
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
                                latch.count_down();
                            },
                            .wg = nullptr,
                            .name = "worker::expand_query",
                    })
            );

        }
        latch.wait();
        task.ctx_cols.clear();

        matops->to_ntt(query_mat.data);
    }

    math_utils::matrix<seal::Ciphertext> RowMultiplicationStrategy::multiply_rows(WorkerServiceTask &task) {
        auto start = std::chrono::high_resolution_clock::now();

        // i have queries, which are columns to multiply with the rows.
        // i set the task's ptxs in a matrix of the correct size.
        auto mat_size = int(task.ptx_rows.size());
        math_utils::matrix<seal::Plaintext> ptxs(mat_size, task.row_size);

        // moving from a map of rows to a matrix.
        auto row = -1;
        for (auto &pair: task.ptx_rows) {
            row += 1;
            auto ptx_row = pair.second;
            for (std::uint64_t col = 0; col < ptx_row.size(); ++col) {
                ptxs(row, col) = std::move(ptx_row[col]);
            }
        }

        math_utils::matrix<seal::Ciphertext> result;
        matops->multiply(ptxs, query_mat, result);

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "worker::multiplication time: " << duration.count() << "ms" << std::endl;

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
            for (auto &p: task.ptx_rows) {
                row_to_index[++row] = p.first;
            }

            // before sending the response - ensure we send it in reg-form to compress the data a bit more.
            matops->from_ntt(computed.data);

            // Note that we pass this by reference to the threads. so moving this about is dangerous.
            std::vector<std::shared_ptr<concurrency::promise<distribicom::MatrixPart>>> to_send_promises;
            to_send_promises.reserve(computed.rows * computed.cols);

            auto promise_index = -1;
            for (uint64_t i = 0; i < computed.rows; ++i) {
                for (uint64_t j = 0; j < computed.cols; ++j) {

                    to_send_promises.emplace_back(
                            std::make_shared<concurrency::promise<distribicom::MatrixPart>>(1, nullptr));

                    promise_index++;
                    pool->submit(
                            {
                                    .f=[&, promise_index, i, j]() {
                                        auto part = std::make_unique<distribicom::MatrixPart>();
                                        part->set_row(row_to_index.at(int(i)));
                                        part->set_col(col_to_query_index.at(int(j)));
                                        part->mutable_ctx()->set_data(mrshl->marshal_seal_object(computed(i, j)));

                                        to_send_promises[promise_index]->set(std::move(part));
                                    },
                                    .wg = to_send_promises[promise_index]->get_latch(),
                                    .name = "worker::send_response",
                            }
                    );

                }
            }

            grpc::ClientContext context;
            utils::add_metadata_string(context, constants::credentials_md, sym_key);
            utils::add_metadata_size(context, constants::round_md, task.round);
            utils::add_metadata_size(context, constants::epoch_md, task.epoch);

            distribicom::Ack resp;
            auto stream = manager_conn->ReturnLocalWork(&context, &resp);


            random_wait_time();
            for (auto &p: to_send_promises) {
                //Enable write batching in streams (message k + 1 does not rely on responses from message k)
                stream->Write(*p->get(), grpc::WriteOptions().set_buffer_hint());
            }

            stream->WritesDone();
            auto status = stream->Finish();
            if (!status.ok()) {
                std::cerr
                        << "Services::RowMultiplicationStrategy:: hadn't completed its stream:" + status.error_message()
                        << std::endl;
            }
        });
        std::cout << "worker::sending time: " << sending_time << "ms" << std::endl;
    }
}
