#pragma once

#include "seal/seal.h"
#include "distribicom.pb.h"
#include "constants.hpp"
#include <string>
#include <memory>
#include <thread>
#include "concurrency/channel.hpp"
#include "marshal/marshal.hpp"
#include "math_utils/query_expander.hpp"
#include "worker_strategy.hpp"
#include "authenticator.hpp"

namespace services {

    class Manager; // Forward decl

    /**
     * Worker - Shared Memory Implementation
     * Receives tasks via Inbox (Channel) and sends results via Manager::SubmitResult
     */
    class Worker {
        // used by the worker to ensure it receives authentic data.
        std::vector<std::byte> symmetric_secret_key;

        // states how this worker will operate.
        distribicom::AppConfigs appcnfgs;

        concurrency::Channel<WorkerServiceTask> chan;
        std::shared_ptr<marshal::Marshaller> mrshl;
        std::vector<std::thread> threads;
        
        // --- Shared Memory Components ---
        std::shared_ptr<concurrency::Channel<std::shared_ptr<distribicom::WorkerTaskPart>>> inbox_;
        std::weak_ptr<Manager> manager_;
        bool running_ = false;

    public:
        // Responder Helper Methods
        void start_response(int round, int epoch, double compute_time);
        void send_part(const distribicom::MatrixPart& part);
        void finish_response();
        
        // Getters for Manager to register
        std::string get_credential_id() const { return credential_id_; }
        std::shared_ptr<concurrency::Channel<std::shared_ptr<distribicom::WorkerTaskPart>>> get_inbox() { return inbox_; }

        // making this public for now, so that we can test it.
        std::shared_ptr<work_strategy::RowMultiplicationStrategy> strategy;

        explicit Worker(distribicom::AppConfigs &&wcnfgs);
        virtual ~Worker();

        // Sets the manager implementation to callback
        void set_manager(std::weak_ptr<Manager> mgr) { manager_ = mgr; }

        // Replaces RunSession - Main Loop
        void run_session();

        void close();

    private:
        Authenticator authenticator_;
        std::string credential_id_;
        
        // Helpers
        void setup_strategies();
        void process_incoming_task(std::shared_ptr<distribicom::WorkerTaskPart> task_ptr);
        void update_current_task(const distribicom::MatrixPart &tmp); // If needed or refactor logic
        
        int current_round_ = 0;
        int current_epoch_ = 0;
    };
}
