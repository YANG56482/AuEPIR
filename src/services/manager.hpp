#pragma once

#include "seal/seal.h"
#include "distribicom.pb.h"
#include "distribicom.grpc.pb.h"
#include "math_utils/matrix.h"

#include "math_utils/matrix_operations.hpp"
#include "math_utils/query_expander.hpp"
#include "marshal/marshal.hpp"
#include "concurrency/concurrency.h"
#include "utils.hpp"
#include "client_context.hpp"

#include "manager_workstream.hpp"


namespace {
    template<typename T>
    using promise = concurrency::promise<T>;
}


namespace services {

    struct ResultMatPart {
        seal::Ciphertext ctx;
        std::uint64_t row;
        std::uint64_t col;
    };

    struct WorkerInfo {
        std::uint64_t worker_number;
        std::uint64_t group_number;
        std::uint64_t query_range_start;
        std::uint64_t query_range_end;
        std::vector<std::uint64_t> db_rows;
    };

    /**
    * WorkDistributionLedger keeps track on a distributed task for a single round.
    * it should keep hold on a working task.
    */
    struct WorkDistributionLedger {
        std::shared_mutex mtx;
        // states the workers that have already contributed their share of the work.
        std::set<std::string> contributed;

        // an ordered list of all workers this ledger should wait to.
        std::vector<std::string> worker_list;

        // result_mat is the work result of all workers.
        math_utils::matrix<seal::Ciphertext> result_mat;

        // stored in ntt form.
        std::map<std::uint64_t, math_utils::matrix<seal::Ciphertext>> db_x_queries_x_randvec;

        std::map<std::string, std::unique_ptr<concurrency::promise<bool>>> worker_verification_results;
        // open completion will be closed to indicate to anyone waiting.
        concurrency::Channel<int> done;
    };

    struct EpochData {
        std::shared_ptr<WorkDistributionLedger> ledger;
        std::map<std::string, WorkerInfo> worker_to_responsibilities;

        // following the same key as the client's db. [NTT FORM]
        std::map<std::uint64_t, std::shared_ptr<math_utils::matrix<seal::Ciphertext>>> queries_dim2;

        // the following vector will be used to be multiplied against incoming work.
        std::shared_ptr<std::vector<std::uint64_t>> random_scalar_vector;

        // key is group
        // contains promised computation for expanded_queries X random_scalar_vector [NTT FORM]
        std::map<std::uint64_t, std::shared_ptr<math_utils::matrix<seal::Ciphertext>>> query_mat_times_randvec;

        std::uint64_t num_freivalds_groups{};

        std::uint64_t size_freivalds_group{};
    };


    class Manager : distribicom::Manager::WithCallbackMethod_RegisterAsWorker<distribicom::Manager::Service> {
    private:
        std::uint64_t thread_unsafe_compute_number_of_groups() const;

        distribicom::AppConfigs app_configs;

        std::shared_mutex mtx;

        // yth
        std::set<std::string> current_epoch_blacklist;

        void recover_worker_computation(const std::string &worker_creds);
        // yth

        std::shared_ptr<concurrency::threadpool> pool;

        concurrency::Counter worker_counter;

        std::shared_ptr<marshal::Marshaller> marshal;
        std::shared_ptr<math_utils::MatrixOperations> matops;
        std::shared_ptr<math_utils::QueryExpander> expander;
        std::unique_ptr<distribicom::WorkerTaskPart> completion_message;
        std::unique_ptr<distribicom::WorkerTaskPart> rnd_msg;

        math_utils::matrix<std::unique_ptr<distribicom::WorkerTaskPart>> marshall_db;

        // TODO: use friendship, instead of ifdef!
#ifdef DISTRIBICOM_DEBUG
    public: // making the data here public: for debugging/testing purposes.
#endif
        std::map<std::string, WorkStream *> work_streams;
        EpochData epoch_data;//@todo refactor into pointer and use atomics


    public:
        ClientDB client_query_manager;
        services::DB<seal::Plaintext> db;

        explicit Manager() : pool(std::make_shared<concurrency::threadpool>()), db(1, 1) {
            completion_message = std::make_unique<distribicom::WorkerTaskPart>();
            completion_message->set_task_complete(true);
            rnd_msg = std::make_unique<distribicom::WorkerTaskPart>();

        };

        explicit Manager(const distribicom::AppConfigs &app_configs, std::map<uint32_t,
            std::unique_ptr<services::ClientInfo>> &client_db, math_utils::matrix<seal::Plaintext> &db) :
            app_configs(app_configs),
            pool(std::make_shared<concurrency::threadpool>()),
            marshal(marshal::Marshaller::Create(utils::setup_enc_params(app_configs))),
            matops(math_utils::MatrixOperations::Create(
                       math_utils::EvaluatorWrapper::Create(
                           utils::setup_enc_params(app_configs)
                       ), pool
                   )
            ),
            expander(math_utils::QueryExpander::Create(
                         utils::setup_enc_params(app_configs),
                         pool
                     )
            ),
            db(db) {
            this->client_query_manager.client_counter = client_db.size();
            this->client_query_manager.id_to_info = std::move(client_db);
            completion_message = std::make_unique<distribicom::WorkerTaskPart>();
            completion_message->set_task_complete(true);
            rnd_msg = std::make_unique<distribicom::WorkerTaskPart>();
            marshall_db = math_utils::matrix<std::unique_ptr<distribicom::WorkerTaskPart>>(db.rows, db.cols);
            for (std::uint64_t i = 0; i < marshall_db.rows; ++i) {
                for (std::uint64_t j = 0; j < marshall_db.cols; ++j) {
                    marshall_db.data[marshall_db.pos(i, j)] = std::make_unique<distribicom::WorkerTaskPart>();
                    marshall_db.data[marshall_db.pos(i, j)]->mutable_matrixpart()->set_row(i);
                    marshall_db.data[marshall_db.pos(i, j)]->mutable_matrixpart()->set_col(j);
                }
            }
        };


        // a worker should send its work, along with credentials of what it sent.
        ::grpc::Status
        ReturnLocalWork(::grpc::ServerContext *context, ::grpc::ServerReader<::distribicom::MatrixPart> *reader,
                        ::distribicom::Ack *response) override;


        bool verify_row(std::shared_ptr<math_utils::matrix<seal::Ciphertext>> &workers_db_row_x_query,
                        std::uint64_t row_id, std::uint64_t group_id, std::uint64_t client_id);

        void
        async_verify_worker(
            const std::shared_ptr<std::vector<std::unique_ptr<concurrency::promise<ResultMatPart>>>> parts_ptr,
            const std::string worker_creds);

        // void put_in_result_matrix(const std::vector<std::unique_ptr<concurrency::promise<ResultMatPart>>> &parts);
        // YTH
        void put_in_result_matrix(const std::vector<std::unique_ptr<concurrency::promise<ResultMatPart>>> &parts, const std::string &worker_creds);
        // YTH

        void calculate_final_answer();;


        // todo: break up query distribution, create unified structure for id lookups, modify ledger accoringly

        std::shared_ptr<WorkDistributionLedger> distribute_work(const ClientDB &all_clients, int rnd, int epoch);

        void wait_for_workers(int i);

        /**
         *  assumes num workers map well to db and queries
         */
        std::map<std::string, WorkerInfo> map_workers_to_responsibilities(uint64_t num_queries);

        void send_galois_keys(const ClientDB &all_clients);

        void send_db(int rnd, int epoch);

        void send_queries(const ClientDB &all_clients);

        ::grpc::ServerWriteReactor<::distribicom::WorkerTaskPart> *RegisterAsWorker(
            ::grpc::CallbackServerContext *ctx/*context*/,
            const ::distribicom::WorkerRegistryRequest *rqst/*request*/) override;

        void close() {
            mtx.lock();
            for (auto ptr: work_streams) {
                ptr.second->close();
            }
            mtx.unlock();
        }

        /**
         * assumes the given db is thread-safe.
         */
        void new_epoch(const ClientDB &db);

        std::shared_ptr<WorkDistributionLedger>
        new_ledger(const ClientDB &all_clients);

        /**
         * Waits on freivalds verify, returns (if any) parts that need to be re-evaluated.
         */
        void wait_on_verification();

        void freivald_preprocess(EpochData &ed, const ClientDB &db);
    };
}
