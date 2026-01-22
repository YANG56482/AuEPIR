#pragma once

#include "distribicom.pb.h" // Only standard protobuf needed, not gRPC

namespace services {
    
    class ISender {
    public:
        using cleanup_callback_t = std::function<void()>;
        
        virtual ~ISender() = default;
        virtual void add_task_to_write(distribicom::WorkerTaskPart *tsk) = 0;
        virtual void write_next() = 0;
        virtual void close() = 0;
        virtual void set_cleanup_callback(cleanup_callback_t callback) = 0;
    };
}
