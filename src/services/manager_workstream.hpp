#include <grpc++/grpc++.h>
#include "distribicom.pb.h"
#include "queue"
#include "mutex"


#include "sender_interface.hpp"

namespace services {
    class WorkStream : public grpc::ServerWriteReactor<distribicom::WorkerTaskPart>, public ISender {
        std::mutex mtx;
        std::queue<distribicom::WorkerTaskPart *> to_write;
        bool mid_write = false; // ensures that sequential calls to write would not write the same item twice.
        bool finished = false;

        std::function<void()> cleanup_callback;
        
        void OnDone() override {
            std::cout << "Manager::WorkStream: Closing stream" << std::endl;
            if (cleanup_callback) {
                cleanup_callback();
            }
        }

        void OnWriteDone(bool ok) override;

    public:

        void close() override;

        void add_task_to_write(distribicom::WorkerTaskPart *tsk) override;

        void write_next();


        void set_cleanup_callback(std::function<void()> callback) override {
            cleanup_callback = std::move(callback);
        }



    private:
        // Batching Logic
        std::vector<std::unique_ptr<distribicom::WorkerTaskPart>> owned_messages;
        distribicom::MatrixPartBatch pending_batch;
        size_t batch_threshold = 100; // Adjust based on message size
        
        void flush_batch();

    };
}
