#pragma once

#include "concurrency/channel.hpp"
#include "sender_interface.hpp"
#include "distribicom.pb.h"
#include <memory>
#include <iostream>

namespace services {

    // Forward declaration
    class Worker;

    // A simple wrapper around concurrency::Channel to provide an ISender interface
    // but utilizing shared_ptr for zero-copy.
    class SharedMemorySender : public ISender {
        std::shared_ptr<concurrency::Channel<std::shared_ptr<distribicom::WorkerTaskPart>>> outbox_;
        std::string worker_id_;

    public:
        SharedMemorySender(std::shared_ptr<concurrency::Channel<std::shared_ptr<distribicom::WorkerTaskPart>>> outbox, std::string worker_id) 
            : outbox_(outbox), worker_id_(worker_id) {}

        void add_task_to_write(distribicom::WorkerTaskPart* task) override {
            // In shared memory, we just copy the pointer if it's already a shared_ptr, 
            // but the interface provides a raw pointer (from existing gRPC logic).
            // For optimal performance, we should change the upper layers to pass shared_ptr.
            // For now, to minimize refactoring depth, we make a copy into a shared_ptr 
            // OR if we know the source lifecycle is managed, we could optimize.
            // However, protobufs have copy semantics usually. 
            // Ideally: The upstream Manager::send_db creates a UNIQUE or SHARED ptr.
            
            // Current Manager implementation doing: stream->add_task_to_write(rnd_msg.get());
            // So we must copy it because the original `rnd_msg` is reused/modified by Manager.
            
            auto task_copy = std::make_shared<distribicom::WorkerTaskPart>(*task);
            outbox_->write(std::move(task_copy));
        }

        // Overload for optimization if we have a shared_ptr already
        // This requires casting ISender down or modifying ISender. 
        // For now, we stick to the interface or add a new method if we modify ISender.
        void add_task_to_write_shared(std::shared_ptr<distribicom::WorkerTaskPart> task) {
             outbox_->write(std::move(task));
        }

        void write_next() override {
            // No-op in Channel model as write() pushes immediately.
            // gRPC needed this to actually flush/step the stream.
        }

        void close() override {
            outbox_->close();
            if (cleanup_callback_) {
                cleanup_callback_();
            }
        }

        void set_cleanup_callback(cleanup_callback_t callback) override {
            cleanup_callback_ = callback;
        }

    private:
        cleanup_callback_t cleanup_callback_;
    };

}
