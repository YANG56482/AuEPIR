#include "manager.hpp"
#include <grpc++/grpc++.h>
#include "utils.hpp"
// #include "seal/seal.h"

#define UNUSED(x) (void)(x)

namespace services {

    ::grpc::Status Manager::ReturnLocalWork(::grpc::ServerContext *context,
                                            ::grpc::ServerReader<::distribicom::MatrixPart> *reader,
                                            ::distribicom::Ack *resp) {
        UNUSED(resp);
        try {
            std::string worker_creds = utils::extract_string_from_metadata(
                    context->client_metadata(),
                    constants::credentials_md
            );

            mtx.lock_shared();
            auto exists = work_streams.find(worker_creds) != work_streams.end();
            mtx.unlock_shared();

            if (!exists) {
                return {grpc::StatusCode::INVALID_ARGUMENT, "worker not registered"};
            }

            std::cout << "Manager::ReturnLocalWork::receiving work." << std::endl;
            distribicom::MatrixPart tmp;
            auto parts = std::make_shared<std::vector<std::unique_ptr<concurrency::promise<ResultMatPart>>>>();
            auto &parts_vec = *parts;
            while (reader->Read(&tmp)) {

                auto moved_ptr = std::make_shared<distribicom::MatrixPart>(std::move(tmp));
                parts_vec.emplace_back(std::move(std::make_unique<concurrency::promise<ResultMatPart>>(1, nullptr)));
                auto latest = parts_vec.size() - 1;
                pool->submit(
                        {
                                .f = [&, moved_ptr, latest]() {

                                    parts_vec[latest]->set(
                                            std::make_unique<ResultMatPart>(
                                                    std::move(
                                                            ResultMatPart{
                                                                    std::move(
                                                                            marshal->unmarshal_seal_object<seal::Ciphertext>(
                                                                                    moved_ptr->ctx().data())
                                                                    ),
                                                                    moved_ptr->row(),
                                                                    moved_ptr->col()
                                                            }
                                                    )
                                            )
                                    );

                                },
                                .wg = parts_vec[latest]->get_latch(),
                                .name = "Manager::ReturnLocalWork::unmarshal",
                        }
                );
            }

#ifdef FREIVALDS
            async_verify_worker(parts, worker_creds);
#endif

            // put_in_result_matrix(parts_vec);
            // YTH
            put_in_result_matrix(parts_vec, worker_creds);
            // YTH

            auto ledger = epoch_data.ledger;

            ledger->mtx.lock();
            ledger->contributed.insert(worker_creds);
            auto n_contributions = ledger->contributed.size();
            ledger->mtx.unlock();

            // signal done:
            if (n_contributions == ledger->worker_list.size()) {
                std::cout << "Manager::ReturnLocalWork: all workers have contributed." << std::endl;
                ledger->done.close();
            }
        } catch (const std::exception &e) {
            std::cerr << "Manager::ReturnLocalWork: " << e.what() << std::endl;
            return {grpc::StatusCode::INTERNAL, e.what()};
        }
        return {};
    }

    std::shared_ptr<WorkDistributionLedger>
    Manager::distribute_work(const ClientDB &all_clients, int rnd, int epoch) {
        epoch_data.ledger = new_ledger(all_clients);

        if (rnd == 0) {
            std::cout << "Manager::distribute_work: sending queries" << std::endl;
            send_queries(all_clients);
        }

        std::cout << "Manager::distribute_work: sending db" << std::endl;
        send_db(epoch, 0);

        return epoch_data.ledger;
    }

    std::shared_ptr<WorkDistributionLedger>
    Manager::new_ledger(const ClientDB &all_clients) {
        auto ptx_db = db.many_reads();
        auto ledger = std::make_shared<WorkDistributionLedger>();

#ifdef FREIVALDS
        for (size_t i = 0; i < epoch_data.num_freivalds_groups; i++) {
            // need to compute DB X epoch_data.query_matrix.
            matops->multiply(ptx_db.mat, *epoch_data.query_mat_times_randvec[i], ledger->db_x_queries_x_randvec[i]);
            matops->from_ntt(ledger->db_x_queries_x_randvec[i].data);
        }
#endif

        ledger->worker_list = std::vector<std::string>();

        std::shared_lock lock(mtx);
        ledger->worker_list.reserve(work_streams.size());
        for (auto &worker: work_streams) {

            // yth
            if(current_epoch_blacklist.count(worker.first)) {
                continue;
            }
            // yth

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
            auto ptx_db = db.many_reads(); // sent to threads via ref, dont exit function without waiting on threads.
            marshal->marshal_seal_ptxs(ptx_db.mat.data, marshall_db.data);


            distribicom::Ack response;
            std::shared_lock lock(mtx);
            rnd_msg->mutable_md()->set_round(rnd);
            rnd_msg->mutable_md()->set_epoch(epoch);

            // yth
            size_t active_workers_count = 0;
            for(const auto &[name, stream]: work_streams) {
                if(current_epoch_blacklist.find(name) == current_epoch_blacklist.end()) {
                    active_workers_count++;
                }
            }
            auto latch = std::make_shared<concurrency::safelatch>(active_workers_count);

            // auto latch = std::make_shared<concurrency::safelatch>(work_streams.size());
            // yth

            
            for (auto &[name, stream]: work_streams) {
                

                // yth
                if(current_epoch_blacklist.count(name)) {
                    continue;
                }
                // yth

                pool->submit(
                        {
                                .f = [&, name, stream]() {
                                    stream->add_task_to_write(rnd_msg.get());

                                    auto db_rows = epoch_data.worker_to_responsibilities[name].db_rows;
                                    for (const auto &db_row: db_rows) {
                                        // first send db
                                        for (std::uint32_t j = 0; j < ptx_db.mat.cols; ++j) {

                                            marshall_db(db_row, j)->mutable_matrixpart()->set_row(db_row);
                                            marshall_db(db_row, j)->mutable_matrixpart()->set_col(j);

                                            stream->add_task_to_write(marshall_db(db_row, j).get());
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
        });
        std::cout << "Manager::send_db: " << time << "ms" << std::endl;
    }


    void
    Manager::send_queries(const ClientDB &all_clients) {
        auto time = utils::time_it([&]() {
            std::shared_lock lock(mtx);

            auto latch = std::make_shared<concurrency::safelatch>(work_streams.size());
            for (auto &[name, stream]: work_streams) {
                pool->submit(
                        {
                                .f = [&, name, stream]() {
                                    auto current_worker_info = epoch_data.worker_to_responsibilities[name];
                                    auto range_start = current_worker_info.query_range_start;
                                    auto range_end = current_worker_info.query_range_end;

                                    for (std::uint64_t i = range_start;
                                         i < range_end; ++i) { //@todo currently assume that query has one ctext in dim

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
            latch->wait();
        });

        std::cout << "Manager::send_queries: " << time << "ms" << std::endl;
    }


    void Manager::wait_for_workers(int i) {
        worker_counter.wait_for(i);
    }

    void Manager::send_galois_keys(const ClientDB &all_clients) {
        auto time = utils::time_it([&]() {
            std::shared_lock lock(mtx);

            auto latch = std::make_shared<concurrency::safelatch>(work_streams.size());
            for (auto &[name, stream]: work_streams) {

                pool->submit(
                        {
                                .f=[&, name, stream]() {
                                    auto range_start = epoch_data.worker_to_responsibilities[name].query_range_start;
                                    auto range_end = epoch_data.worker_to_responsibilities[name].query_range_end;

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

    std::map<std::string, WorkerInfo> Manager::map_workers_to_responsibilities(std::uint64_t num_queries) {
        // Assuming more workers than rows.
        std::uint64_t num_groups = thread_unsafe_compute_number_of_groups();

        auto num_workers_in_group = work_streams.size() / num_groups;

        // num groups is the amount of duplication of the DB.
        auto num_queries_per_group = num_queries / num_groups; // assume num_queries>=num_groups

        auto num_rows_per_worker = app_configs.configs().db_rows() / num_workers_in_group;
        if (num_rows_per_worker == 0) {
            num_rows_per_worker = 1;
        }

        std::map<std::string, WorkerInfo> worker_to_responsibilities;
        std::uint64_t i = 0;
        std::uint64_t group_id = -1;
        for (auto const &[worker_name, stream]: work_streams) {
            (void) stream; // not using val.

            if (i % num_workers_in_group == 0) {
                group_id++;
            }
            auto worker_id = i++;

            // group_id determines that part of queries each worker receives.
            auto range_start = group_id * num_queries_per_group;
            auto range_end = range_start + num_queries_per_group;

            // worker should get a range of all rows:
            // I want each worker to have a sequential range of rows for each worker.

            std::vector<std::uint64_t> db_rows;
            db_rows.reserve(num_rows_per_worker);

            auto partition_start = (worker_id % num_workers_in_group) * num_rows_per_worker;
            auto num_rows_to_give = num_rows_per_worker;
            // notice we already advanced i, so we check i %num_workers_in_group and not i+1%num_workers_in_group
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
        }

        std::cout << "Manager::map_workers_to_responsibilities output:" << std::endl;
        std::cout << "{" << std::endl;
        std::cout << "  num_groups: " << num_groups << ", " << std::endl;
        std::cout << "  num_workers_in_group: " << num_workers_in_group << ", " << std::endl;
        std::cout << "  num_queries_per_group: " << num_queries_per_group << ", " << std::endl;
        std::cout << "  num_rows_per_worker: " << num_rows_per_worker << ", " << std::endl;
        std::cout << "}" << std::endl;

        return worker_to_responsibilities;
    }

    std::uint64_t Manager::thread_unsafe_compute_number_of_groups() const {
        return std::uint64_t(std::max(size_t(1), work_streams.size() / app_configs.configs().db_rows()));
    }

    ::grpc::ServerWriteReactor<::distribicom::WorkerTaskPart> *
    Manager::RegisterAsWorker(::grpc::CallbackServerContext *ctx, const ::distribicom::WorkerRegistryRequest *rqst) {
        UNUSED(rqst);
        std::string creds = utils::extract_string_from_metadata(ctx->client_metadata(), constants::credentials_md);
        // Assuming for now, no one leaves !
        auto stream = new WorkStream();

        mtx.lock();
        work_streams[creds] = stream;
        std::cout << "Manager::RegisterAsWorker: num workers registered: " << work_streams.size() << std::endl;
        mtx.unlock();

        worker_counter.add(1);

        return stream;
    }

    void randomise_scalar_vec(std::vector<std::uint64_t> &vec) {
        seal::Blake2xbPRNGFactory factory;
        auto prng = factory.create({(std::random_device()) ()});
        std::uniform_int_distribution<unsigned long long> dist(
                std::numeric_limits<uint64_t>::min(),
                std::numeric_limits<uint64_t>::max() >> 5 /* allowed max is 60bit.*/
        );

        for (auto &i: vec) { i = prng->generate(); }
    }

    void Manager::new_epoch(const ClientDB &db) {
        auto time = utils::time_it([&]() {
            auto num_freivalds_groups = thread_unsafe_compute_number_of_groups();
            auto size_freivalds_group = db.client_counter / num_freivalds_groups;

            EpochData ed{
                    .worker_to_responsibilities = map_workers_to_responsibilities(db.client_counter),
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

                // setting dim2 in matrix<seal::ciphertext> and ntt form.
                auto p = std::make_shared<concurrency::promise<math_utils::matrix<seal::Ciphertext>>>(1, nullptr);
                pool->submit(
                        {
                                .f = [&, p]() {
                                    auto mat = std::make_shared<math_utils::matrix<seal::Ciphertext>>
                                            (
                                                    1, expand_size_dim2, // row vec.
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

            // yth
            current_epoch_blacklist.clear();
            std::cout << "[YTH] New Epoch Started. Blacklist cleared." << std::endl;
            // yth

            mtx.unlock();
        });
        std::cout << "Manager::new_epoch: " << time << " ms" << std::endl;
    }

    void Manager::freivald_preprocess(EpochData &ed, const ClientDB &cdb) {
#ifdef FREIVALDS // Freivalds-preprocessing
        auto expand_size = app_configs.configs().db_cols();
        std::vector<std::shared_ptr<concurrency::promise<std::vector<seal::Ciphertext>>>> qs(cdb.id_to_info.size());

        for (const auto &info: cdb.id_to_info) {
            // promises for expanded queries

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
        std::shared_lock lock(epoch_data.ledger->mtx);

        for (const auto &v: epoch_data.ledger->worker_verification_results) {
            auto is_valid = *(v.second->get());
            if (!is_valid) {
                throw std::runtime_error("wait_on_verification:: invalid verification");
            }
        }
    }

    void Manager::put_in_result_matrix(const std::vector<std::unique_ptr<concurrency::promise<ResultMatPart>>> &parts, const std::string &worker_creds) {
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
                                    matops->w_evaluator->transform_to_ntt_inplace(*embedded);
                                    embeddeds[i]->set(std::move(embedded));
                                },
                                .wg = embeddeds[i]->get_latch(),
                                .name = "Manager::put_in_result_matrix"
                        }
                );
            }


            client_query_manager.mutex->lock_shared();

            // YTH
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
            // YTH

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
                promises.insert(
                        {
                                client.first,
                                matops->async_mat_mult(current_query, client.second->partial_answer)
                        }
                );
            }
            for (const auto &client: client_query_manager.id_to_info) {
                client.second->final_answer = std::move(promises[client.first]->get());

                auto to_ntt_time = utils::time_it([&]() {
                    matops->from_ntt(client.second->final_answer->data);
                });

                if (client.first == 0) {
                    std::cout << "Manager::calculate_final_answer:to_ntt sample time: " << to_ntt_time << " ms"
                              << std::endl;
                }
            }


        }
        catch (const std::exception &e) {
            std::cerr << "Manager::calculate_final_answer::exception: " << e.what() << std::endl;
        }
    }

    // bool
    // Manager::verify_row(std::shared_ptr<math_utils::matrix<seal::Ciphertext>> &workers_db_row_x_query,
    //                     std::uint64_t row_id,
    //                     std::uint64_t group_id) {
    //     try {

    //         auto db_row_x_query_x_challenge_vec = matops->scalar_dot_product(workers_db_row_x_query,
    //                                                                          epoch_data.random_scalar_vector);
    //         const auto &expected_result = epoch_data.ledger->db_x_queries_x_randvec[group_id].data[row_id];
    //         matops->w_evaluator->evaluator->sub_inplace(db_row_x_query_x_challenge_vec->data[0], expected_result);


    //         std::cout << "[DEBUG] Ciphertext Size: " << db_row_x_query_x_challenge_vec->data[0].size() << std::endl;


    //         const bool is_row_valid = db_row_x_query_x_challenge_vec->data[0].is_transparent();


    //         return is_row_valid;
    //     } catch (std::exception &e) {
    //         std::cerr << "Manager::verify_row::exception: " << e.what() << std::endl;
    //         return false;
    //     }
    // }

    bool
    Manager::verify_row(std::shared_ptr<math_utils::matrix<seal::Ciphertext>> &workers_db_row_x_query,
                    std::uint64_t row_id,
                    std::uint64_t group_id,
                    std::uint64_t client_id) {
    try {
        auto db_row_x_query_x_challenge_vec = matops->scalar_dot_product(workers_db_row_x_query,
                                                                         epoch_data.random_scalar_vector);

        const auto &expected_result = epoch_data.ledger->db_x_queries_x_randvec[group_id].data[row_id];

        // matops->w_evaluator->evaluator->transform_from_ntt_inplace(db_row_x_query_x_challenge_vec->data[0]);

        matops->w_evaluator->evaluator->sub_inplace(db_row_x_query_x_challenge_vec->data[0], expected_result);

        auto client_ptr = client_query_manager.id_to_info.at(client_id)->local_client_instance;
        
        if (client_ptr) {
            bool is_valid = client_ptr->decrypt_and_verify(db_row_x_query_x_challenge_vec->data[0]);
            return is_valid;
        } else {
            std::cerr << "Error: Local client instance not found for simulation!" << std::endl;
            return false;
        }
    } catch (std::exception &e) {
        std::cerr << "Manager::verify_row::exception: " << e.what() << std::endl;
        return false;
    }
}


    // YTH
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
                matops->w_evaluator->transform_to_ntt_inplace(*embedded);

                client_query_manager.mutex->lock(); 

                auto &partial_answer_mat = *client_info->partial_answer;

                if (embedded->size() > partial_answer_mat.cols) {
                    std::cerr << "[YTH Critical] Recovered embedded size " << embedded->size() 
                              << " > matrix cols " << partial_answer_mat.cols << std::endl;
                }

                for(size_t j=0; j < embedded->size() && j < partial_answer_mat.cols; ++j) {
                    partial_answer_mat(row_id, j) = std::move((*embedded)[j]);
                }

                client_query_manager.mutex->unlock();
            }
        }
        std::cout << "[Recovery] Successfully recovered computation for worker: " << worker_creds << std::endl;
    }
    // YTH

    void Manager::async_verify_worker(
            const std::shared_ptr<std::vector<std::unique_ptr<concurrency::promise<ResultMatPart>>>> parts_ptr,
            const std::string worker_creds) {
        pool->submit(
                {
                        .f=[&, parts_ptr, worker_creds]() {
                            auto &parts = *parts_ptr;

                            auto work_responsibility = epoch_data.worker_to_responsibilities[worker_creds];
                            auto rows = work_responsibility.db_rows;


                            std::cout << "[DEBUG] Verifying Worker: " << worker_creds 
                                      << ", Responsibility Rows: " << rows.size() << std::endl;


                            auto query_row_len =
                                    work_responsibility.query_range_end - work_responsibility.query_range_start;

#ifdef DISTRIBICOM_DEBUG
                            if (query_row_len != epoch_data.size_freivalds_group) {
                                throw std::runtime_error(
                                        "unimplemented case: query_row_len != epoch_data.size_freivalds_group");
                            }
#endif
                            auto worker_verify_time = utils::time_it([&]() {

                                for (size_t i = 0; i < rows.size(); i++) {


                                    std::cout << "[DEBUG] Checking row index: " << rows[i] << " (Loop i=" << i << ")" << std::endl;


                                    auto workers_db_row_x_query = std::make_shared<math_utils::matrix<seal::Ciphertext>>(
                                            query_row_len, 1);

                                    auto &mpdata = workers_db_row_x_query->data;
                                    for (size_t j = 0; j < query_row_len; j++) {
                                        mpdata[j] = parts[j + i * query_row_len]->get()->ctx;
                                    }

                                    // auto is_valid = verify_row(workers_db_row_x_query, rows[i],
                                    //                            work_responsibility.group_number);
                                    auto client_id = work_responsibility.query_range_start;
                                    auto is_valid = verify_row(workers_db_row_x_query, rows[i],
                                                            work_responsibility.group_number, 
                                                            client_id);

                                    if (!is_valid) {
                                        std::cout << "[DEBUG] >>> Row " << rows[i] << " INVALID! <<<" << std::endl;
                                    }


                                    if (!is_valid) {

                                        // yth
                                        std::cout << "YTH SECURITY ALERT !!!" << std::endl;
                                        std::cout << "Malicious Worker Detected: " << worker_creds << std::endl;
                                        std::cout << "Verification failed at DB Row: " << rows[i] << std::endl;
                                        // yth

                                        epoch_data.ledger->worker_verification_results[worker_creds]->set(
                                                std::make_unique<bool>(false)

                                        );

                                        // yth
                                        {
                                            std::unique_lock<std::shared_mutex> lock(mtx);
                                            current_epoch_blacklist.insert(worker_creds);
                                            std::cout << "YTH Action: Worker " << worker_creds << " added to CURRENT EPOCH BLACKLIST." << std::endl;
                                        }

                                        try {
                                            recover_worker_computation(worker_creds);
                                            epoch_data.ledger->worker_verification_results[worker_creds]->set(
                                                    std::make_unique<bool>(true));
                                        } catch (const std::exception& e) {
                                            std::cout << "[YTH Recovery Failed] Critical error during recovery: " << e.what() << std::endl;
                                            epoch_data.ledger->worker_verification_results[worker_creds]->set(
                                                    std::make_unique<bool>(false)
                                            );
                                        }
                                        //yth

                                        return;
                                    }
                                }


                                std::cout << "[DEBUG] Worker " << worker_creds << " passed all verification checks." << std::endl;

                                epoch_data.ledger->worker_verification_results[worker_creds]->set(
                                        std::make_unique<bool>(true));
                            });
                            std::cout << "Manager::async_verify_worker::verification time: " << worker_verify_time
                                      << " ms"
                                      << std::endl;
                        },
                        .wg = epoch_data.ledger->worker_verification_results[worker_creds]->get_latch(),
                        .name = "async_verify_worker_lambda"
                }
        );

    }


}