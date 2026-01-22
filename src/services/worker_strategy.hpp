#pragma once

#include <future>
#include <map>
#include <random>
#include <cstdlib>
#include "distribicom.pb.h"
#include "math_utils/matrix_operations.hpp"
#include "math_utils/query_expander.hpp"
#include "marshal/marshal.hpp"
#include "utils.hpp"
#include "distribicom.pb.h"

namespace services {

    struct WorkerServiceTask {
        // metadata worker needs to know.
        int epoch;
        int round;

        // matrices:
        int row_size;
        std::map<int, std::vector<seal::Plaintext>> ptx_rows;
        // TODO: the current item represents one dimension only!
        //      to expand it we might need other parameters to specify which dimension (for example left side query).
        std::map<int, std::vector<seal::Ciphertext>> ctx_cols;
    };
}

namespace services::work_strategy {
    enum type {
        frievalds,
        fft,
    };

    /**
     * A worker strategy is a class that defines how a worker will operate in a given round.
     * Upon constracting a WorkerServiceTask it'll forward the work to the strategy to conclude operations.
     * Once the strategy is done processing the task, it'll return a result to the worker.
     */
     
    class IResponder {
    public:
        virtual ~IResponder() = default;
        virtual void start_response(int round, int epoch, double compute_time) = 0;
        virtual void send_part(const distribicom::MatrixPart& part) = 0;
        virtual void finish_response() = 0;
    };

    class WorkerStrategy {
    
    private:


    protected:
        // TODO: make a unique ptr!
        std::shared_ptr<concurrency::threadpool> pool;
        std::shared_ptr<math_utils::QueryExpander> query_expander;
        std::shared_ptr<math_utils::MatrixOperations> matops;
        std::map<int, seal::GaloisKeys> gkeys;
        std::shared_mutex mu;
        std::shared_ptr<IResponder> responder_;
        double malicious_probability = 0.0;
        
    public:
        explicit WorkerStrategy(const seal::EncryptionParameters &enc_params,
                                std::shared_ptr<IResponder> responder, double malicious_probability = 0.0) noexcept;

        void store_galois_key(seal::GaloisKeys &&keys, int query_position) {
            mu.lock();
            gkeys[query_position] = std::move(keys);
            mu.unlock();
        };

        virtual ~WorkerStrategy() = default;

        virtual void process_task(WorkerServiceTask &&task) = 0;

    };

    class RowMultiplicationStrategy : public WorkerStrategy {
        math_utils::matrix<seal::Ciphertext> query_mat;
        std::map<int, int> col_to_query_index;

        std::shared_ptr<marshal::Marshaller> mrshl;
        std::string sym_key;
        double last_computation_time_ms = 0;

        void expand_queries(WorkerServiceTask &task);

        void parallel_expansions_into_query_mat(WorkerServiceTask &task, std::map<int, int> &map);

    public:
        explicit RowMultiplicationStrategy(const seal::EncryptionParameters &enc_params,
                                           std::shared_ptr<marshal::Marshaller> &m,
                                           std::shared_ptr<IResponder> responder,
                                           std::string &&sym_key,
                                           double malicious_probability = 0.0) :
            WorkerStrategy(enc_params, responder, malicious_probability), mrshl(m), sym_key(std::move(sym_key)) {
        };

        math_utils::matrix<seal::Ciphertext> multiply_rows(WorkerServiceTask &task);

        void send_response(const WorkerServiceTask &task, math_utils::matrix<seal::Ciphertext> &computed);

        // assumes there is data to process in the task.
        void process_task(WorkerServiceTask &&task) override {
            constexpr auto fname = "RowMultiplicationStrategy::process_task: ";
            try {
                expand_queries(task);

                if (cached_db_rows.empty()) { return; }

                std::cout << fname << "multiplying..." << std::endl;
                auto answer = multiply_rows(task);

                if (malicious_probability > 0.0) {
                    static std::random_device rd;
                    static std::mt19937 gen(rd());
                    std::uniform_real_distribution<> dis(0.0, 1.0);

                    if (dis(gen) < malicious_probability) {
                        std::cout << "!!! [YTH MALICIOUS MODE] Injecting Error into Result !!!" << std::endl;
                        
                        if (answer.rows > 0 && answer.cols > 0) {
                            auto& ctx = answer(0, 0);
                            if (ctx.size() > 0) {
                                ctx.data()[0] += 1;
                                std::cout << "!!! [YTH MALICIOUS MODE] Data corrupted successfully !!!" << std::endl;
                            }
                        }
                    }
                }
                // YTH

                send_response(task, answer);
            } catch (std::exception &e) {
                std::cerr << "RowMultiplicationStrategy::process_task: failure: " << e.what() << std::endl;
            }
        }

        std::map<int, std::vector<seal::Plaintext>> cached_db_rows; 

        std::vector<std::shared_ptr<concurrency::promise<distribicom::MatrixPart>>>
        async_marshal_computed(const math_utils::matrix<seal::Ciphertext> &computed, const std::map<int, int> &row_to_index);
    };
}
