#pragma once

#include "manager.hpp"
#include <zmq.hpp>
#include <thread>
#include <atomic>
#include <deque>
#include <mutex>
#include "zmq_protocol.hpp"

namespace services {

class ZmqManager {
    Manager* manager_;
    zmq::context_t ctx_;
    zmq::socket_t socket_;
    std::atomic<bool> running_{false};
    std::thread poller_thread_;

public:
    ZmqManager(Manager* manager, const std::string& bind_addr = "inproc://dpir_server_inproc") 
        : manager_(manager), ctx_(1), socket_(ctx_, zmq::socket_type::router) 
    {
        socket_.bind(bind_addr);
        std::cout << "[ZmqManager] Bound to " << bind_addr << std::endl;
    }

    void start() {
        running_ = true;
        poller_thread_ = std::thread(&ZmqManager::poll_loop, this);
    }

    void stop() {
        running_ = false;
        if (poller_thread_.joinable()) poller_thread_.join();
    }

    std::mutex queue_mtx_;
    std::deque<std::pair<std::string, std::string>> message_queue_;

private:
    void poll_loop() {
        while (running_) {
            // 1. Process Outgoing Queue
            {
                std::lock_guard<std::mutex> lock(queue_mtx_);
                while(!message_queue_.empty()) {
                    auto& item = message_queue_.front();
                    send_internal(item.first, ZmqMsgType::WORKER_TASK_PART, item.second);
                    message_queue_.pop_front();
                }
            }
            
            // 2. Poll/Recv
            zmq::pollitem_t items[] = { { static_cast<void*>(socket_), 0, ZMQ_POLLIN, 0 } };
            zmq::poll(items, 1, std::chrono::milliseconds(0)); 
            
            if (items[0].revents & ZMQ_POLLIN) {
                zmq::message_t identity;
                zmq::message_t type_msg;

                // 1. Identity
                if (!socket_.recv(identity, zmq::recv_flags::dontwait)) continue;
                std::string id_str(static_cast<char*>(identity.data()), identity.size());

                // 2. Type
                if (!socket_.recv(type_msg, zmq::recv_flags::dontwait)) continue;
                ZmqMsgType type = *static_cast<ZmqMsgType*>(type_msg.data());

                if (type == ZmqMsgType::REGISTRY_REQUEST) {
                     zmq::message_t payload;
                     socket_.recv(payload, zmq::recv_flags::dontwait);
                     
                     distribicom::WorkerRegistryRequest req;
                     req.ParseFromArray(payload.data(), payload.size());
                     manager_->register_zmq_worker(req.credentials(), id_str, req.account_public_key());

                } else if (type == static_cast<ZmqMsgType>(11)) { // WORKER_SERVICE_TASK
                     zmq::message_t round_msg, epoch_msg, payload_msg;
                     socket_.recv(round_msg, zmq::recv_flags::dontwait);
                     socket_.recv(epoch_msg, zmq::recv_flags::dontwait);
                     socket_.recv(payload_msg, zmq::recv_flags::dontwait);
                     
                     int round = *static_cast<int*>(round_msg.data());
                     int epoch = *static_cast<int*>(epoch_msg.data());
                     
                     distribicom::MatrixPart part;
                     part.ParseFromArray(payload_msg.data(), payload_msg.size());
                     
                     manager_->handle_zmq_result(id_str, part, round, epoch);
                }
            } else {
                 std::this_thread::yield(); 
            }
        }
    }
    
    void send_internal(const std::string& id, ZmqMsgType type, const std::string& data) {
         try {
            socket_.send(zmq::buffer(id), zmq::send_flags::sndmore);
            uint8_t t = static_cast<uint8_t>(type);
            socket_.send(zmq::buffer(&t, 1), zmq::send_flags::sndmore);
            socket_.send(zmq::buffer(data), zmq::send_flags::none);
         } catch(...) {}
    }
    
    // Helper to send back to identity
    void send_to_worker(const std::string& id, const std::string& data) {
         std::lock_guard<std::mutex> lock(queue_mtx_);
         message_queue_.emplace_back(id, data);
    }
};

}
