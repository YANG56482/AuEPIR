#pragma once

#include "math_utils/matrix.h"
#include "seal/seal.h"
// #include "distribicom.grpc.pb.h"
#include <mutex>
#include <utility>
#include "pir_client.hpp"

namespace services{
    // This class is a wrapper for a matrix that ensures the access to the matrix is thread safe.
    template<typename T>
    class DB {

        // a proper way to receive a matrix from the DB for multiple reads. upon scope exit the DB's lock will be released.
        struct shared_mat {
            explicit shared_mat(const math_utils::matrix<T> &mat, std::mutex &mtx) : mat(mat), lck(mtx) {}

            const math_utils::matrix<T> &mat;
        private:
            std::lock_guard<std::mutex> lck;
        };

        math_utils::matrix<T> memory_matrix;
        std::mutex mtx;

    public:
        // TODO: add a proper way to create PIR DB.
        explicit DB(const std::uint32_t a, const std::uint32_t b) : memory_matrix(), mtx() {}

        // used mainly for testing.
        explicit DB(math_utils::matrix<T> &mat) : memory_matrix(mat), mtx() {}
        
        // move constructor to avoid deep copy
        explicit DB(math_utils::matrix<T> &&mat) : memory_matrix(std::move(mat)), mtx() {}

        // gives access to a locked reference of the matrix.
        // as long as the shared_mat instance is not destroyed the DB remains locked.
        // useful for distributing the DB, or going over multiple rows.
        shared_mat many_reads() {
            return shared_mat(memory_matrix, mtx);
        }

        void transform_inplace(std::function<void(math_utils::matrix<T>&)> func) {
            std::lock_guard<std::mutex> l(mtx);
            func(memory_matrix);
        }

        void write(const T &t, const std::uint32_t row, const std::uint32_t col) {
            mtx.lock();
            memory_matrix(row, col) = t;
            mtx.unlock();
        }

        void
        write(const std::vector<uint64_t> &new_element, const std::uint32_t ptx_num, uint64_t offset, PIRClient
        *replacer_machine);



    //    void write(const std::vector<uint64_t>& new_element, const int ptx_num, uint64_t offset, PIRClient replacer_machine) {
    //        mtx.lock();
    //        if constexpr (std::is_same<T, seal::Plaintext>::value){
    //            memory_matrix.data[ptx_num] = replacer_machine.replace_element(memory_matrix.data[ptx_num], new_element, offset);
    //            mtx.unlock();
    //        }
    //        else {
    //            mtx.unlock();
    //            throw std::runtime_error("unsupported type"); }
    //    }

        // todo: add special write function - for the DB<seal::Plaintext> case - where we can write to specific part
        //  of a plaintext.
        void write() {

        }

        // mainly used for testing.
        bool try_write() {
            auto success = mtx.try_lock();
            if (!success) {
                return false;
            }
            mtx.unlock();
            return true;
        };
    };



};


