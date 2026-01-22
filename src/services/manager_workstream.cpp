
#include "manager_workstream.hpp"

void services::WorkStream::add_task_to_write(distribicom::WorkerTaskPart* tsk) {
    std::lock_guard<std::mutex> lock(mtx);
    if (finished) return; // Don't accept new tasks if finished

    if (tsk->has_matrixpart()) {
        // Buffer this part
        auto* new_part = pending_batch.add_parts();
        new_part->CopyFrom(tsk->matrixpart()); // Deep copy from cached/original message

        if (pending_batch.parts_size() >= batch_threshold) {
            flush_batch();
        }
    } else {
        // Non-matrix message (metadata, completion, etc.)
        // Flush any pending batch first to preserve order
        if (pending_batch.parts_size() > 0) {
            flush_batch();
        }
        // Then direct write
        to_write.emplace(tsk);
    }
}

void services::WorkStream::flush_batch() {
    if (pending_batch.parts_size() == 0) return;

    auto batched_msg = std::make_unique<distribicom::WorkerTaskPart>();
    *batched_msg->mutable_matrixpartbatch() = std::move(pending_batch);
    
    // Store ownership
    to_write.emplace(batched_msg.get());
    owned_messages.push_back(std::move(batched_msg));
    
    // Reset pending batch (moved out state)
    pending_batch.Clear();
}

void services::WorkStream::write_next() {
    std::lock_guard<std::mutex> lock(mtx);
    if (finished) return; // Prevent StartWrite after Finish
    
    if (!mid_write && !to_write.empty()) {
        mid_write = true;
        StartWrite(to_write.front());
    }
}

void services::WorkStream::close() {
    std::lock_guard<std::mutex> lock(mtx);
    if (finished) return;
    finished = true;
    
    // Flush any remaining partial batch
    if (pending_batch.parts_size() > 0) {
        flush_batch();
    }
    
    // Only call Finish if we are NOT in the middle of a write.
    // If we are writing, OnWriteDone will handle the Finish call.
    if (!mid_write) {
        Finish(grpc::Status::OK);
    }
}

void services::WorkStream::OnWriteDone(bool ok) {

    if (!ok) {
        std::cout << "Bad write" << std::endl;
    }

    std::lock_guard<std::mutex> lock(mtx);
    mid_write = false;
    if (!to_write.empty()) { to_write.pop(); }
    
    // If not finished, trigger next write if available
    if (!to_write.empty()) {
        mid_write = true;
        StartWrite(to_write.front());
    } 
    // If finished and no write is in progress, close the stream
    else if (finished) {
        Finish(grpc::Status::OK);
    }
}
