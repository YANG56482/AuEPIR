#pragma once

#include "sender_interface.hpp"
#include "zmq_helper.hpp"
#include <mutex>
#include <queue>
#include <memory> 
#include "zmq_manager.hpp" // Added

namespace services {

// Forward declare ZmqManager or use raw socket?
// Ideally ZmqWorkStream is created by ZmqManager and holds a reference to the socket/manager.
// But ZmqManager owns the socket and is single-threaded (mostly).
// ZMQ Sockets are NOT thread safe.
// So ZmqWorkStream cannot write directly to socket if it's called from multiple threads (Manager logic).
// Manager logic (distribute_work) runs in main thread.
// ZmqManager polling loop runs in separate thread.
// If we write to socket from main thread (ZmqWorkStream::add_task_to_write->flush), we race with poller.

// Solution: ZmqManager should have a thread-safe queue for outgoing messages, or we use a "Internal Pipe" (inproc://sender).
// Or simpler: Protect the socket with a mutex in ZmqManager and expose a thread-safe send method?
// ZMQ sockets are not thread safe even with mutex if one thread is blocked on recv.
// But we use ROUTER/DEALER.

// Better Approach for Performance:
// ZmqWorkStream just queues the message. 
// ZmqManager's thread picks it up and sends it.
// Wait, `add_task_to_write` is called by Manager.
// We want "Millisecond" latency. Queuing adds latency?
// If we use `inproc`, we can create a ephemeral socket for sending? No.

// Let's assume ZmqManager exposes a `send_async` method that puts msg into a concurrent queue, 
// and the poller thread checks this queue before/after polling.
// Or use `zmq_poll` with an eventfd/socket pair to wake up the poller.

// For simplicity and speed in this "Hackathon" mode:
// I will add a `ThreadSafeSender` to ZmqManager.
// Actually, since this is DPIR, `distribute_work` is the main producer.
// If we stop polling during distribution? No.

// Implementation:
// ZmqWorkStream holds a pointer to ZmqManager.
// calls `ZmqManager->send_to_worker(identity, task_part)`.
// ZmqManager implementation of `send_to_worker` will be thread-safe (locking a queue).
// The Poller thread will dequeue and send.

class ZmqManager; // Forward

class ZmqWorkStream : public ISender {
    std::string identity_;
    ZmqManager* manager_;
    cleanup_callback_t cleanup_callback_;
    
public:
    ZmqWorkStream(const std::string& identity, ZmqManager* manager) 
        : identity_(identity), manager_(manager) {}

    void add_task_to_write(distribicom::WorkerTaskPart *tsk) override {
        // Serialize
        std::string payload;
        tsk->SerializeToString(&payload);
        
        // Send via Manager
        if (manager_) {
            manager_->send_to_worker(identity_, payload);
        }
    }

    void close() override {
        if (cleanup_callback_) cleanup_callback_();
    }


    void set_cleanup_callback(cleanup_callback_t callback) override {
        cleanup_callback_ = std::move(callback);
    }
    
    // Support batching similar to WorkStream?
    // ZMQ frames are efficient, but batching small parts (MatrixPart) into larger messages is still good.
    // Reuse existing batching logic?
    // For now, simple pass-through.
};

}
