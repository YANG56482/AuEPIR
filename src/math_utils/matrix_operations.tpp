#include "matrix_operations.hpp"
#include "defines.h"
#include <execution>
#include <latch>

namespace math_utils {

    template<typename T, typename U>
    void verify_not_empty_matrices(const matrix<T> &left, const matrix<U> &right) {
        if (left.data.empty()) {
            throw std::invalid_argument("MatrixOperations::multiply: received empty left matrix");
        }
        if (right.data.empty()) {
            throw std::invalid_argument("MatrixOperations::multiply: received empty right matrix");
        }
    }


    template<typename T, typename U>
    void verify_correct_dimension(const matrix<T> &left, const matrix<U> &right) {
        if (left.cols != right.rows) {
            throw std::invalid_argument("MatrixOperations::multiply: left matrix cols != right matrix rows");
        }
    }

    template<typename U, typename V>
    seal::Ciphertext MatrixOperations::mult_row(uint64_t i, uint64_t j, const matrix<U> &left,
                                                const matrix<V> &right) const {
        seal::Ciphertext tmp;
        seal::Ciphertext tmp_result; // No context init needed if assigned immediately

        // assume that in seal::Plaintext case we don't want to turn into splitPlaintexts
        for (uint64_t k = 0; k < left.cols; ++k) {
            if constexpr ((std::is_same_v<U, seal::Plaintext>)) {
                w_evaluator->mult_reg(left(i, k), right(k, j), tmp);
            } else if constexpr (std::is_same_v<V, seal::Plaintext>) {
                w_evaluator->mult_reg(right(k, j), left(i, k), tmp);
            } else {
                w_evaluator->mult(left(i, k), right(k, j), tmp);
            }

            if (k == 0) {
                tmp_result = tmp;
            } else {
                w_evaluator->add(tmp, tmp_result, tmp_result);
            }
        }
        return tmp_result;
    }

    template<typename U, typename V>
    void
    MatrixOperations::mat_mult(const matrix<U> &left, const matrix<V> &right,
                               matrix<seal::Ciphertext> &result) const {
        verify_correct_dimension(left, right);
        verify_not_empty_matrices(left, right);
        verify_ntt_prior_to_mult(left, right);

        auto wg = std::make_shared<concurrency::safelatch>(int(left.rows * right.cols));
        for (uint64_t i = 0; i < left.rows; ++i) {
            for (uint64_t j = 0; j < right.cols; ++j) {

                pool->submit(
                    {
                        .f = [&, i, j]() { result(i, j) = mult_row(i, j, left, right); },
                        .wg = wg,
                    }
                );

            }
        }
        wg->wait();
    }

    template<typename U, typename V>
    void MatrixOperations::verify_ntt_prior_to_mult(const matrix<U> &left, const matrix<V> &right) const {
        #ifdef DISTRIBICOM_DEBUG
//        // case 1: left is split_ptx and right is ctx
        if constexpr (std::is_same_v<U, SplitPlaintextNTTForm> && std::is_same_v<V, seal::Ciphertext>) {

            if (!left.data[0][0].is_ntt_form()) {
                throw std::invalid_argument("MatrixOperations::multiply: left matrix should be in NTT form");
            }
            if (!right.data[0].is_ntt_form()) {
                throw std::invalid_argument("MatrixOperations::multiply: right matrix should be in NTT form");
            }

        } else if constexpr (std::is_same_v<V, SplitPlaintextNTTForm> && std::is_same_v<U, seal::Ciphertext>) {

            if (!right.data[0][0].is_ntt_form()) {
                throw std::invalid_argument("MatrixOperations::multiply: left matrix should be in NTT form");
            }
            if (!left.data[0].is_ntt_form()) {
                throw std::invalid_argument("MatrixOperations::multiply: right matrix should be in NTT form");
            }

        } else if constexpr (std::is_same_v<U, seal::Plaintext> && std::is_same_v<V, seal::Ciphertext>) {

            if (!left.data[0].is_ntt_form()) {
                throw std::invalid_argument("MatrixOperations::multiply: left matrix should be in NTT form");
            }
            if (!right.data[0].is_ntt_form()) {
                throw std::invalid_argument("MatrixOperations::multiply: right matrix should be in NTT form");
            }

            // case 2: right  is ptx and left is ctx
        } else if constexpr ((std::is_same_v<V, seal::Plaintext> && std::is_same_v<U, seal::Ciphertext>)) {
//            if (right.data[0].is_ntt_form()) {
//                throw std::invalid_argument("MatrixOperations::multiply: left matrix should not be in NTT form");
//            }
//            if (!left.data[0].is_ntt_form()) {
//                throw std::invalid_argument("MatrixOperations::multiply: right matrix should be in NTT form");
//            }
        }
        #endif
    }

    template<typename U, typename V>
    std::unique_ptr<concurrency::promise<matrix<seal::Ciphertext>>>
    MatrixOperations::async_mat_mult(const std::shared_ptr<matrix<U>> &left_ptr,
                                     const std::shared_ptr<matrix<V>> &right_ptr) const { //does not support plaintext mult


        verify_correct_dimension<U, V>(*left_ptr, *right_ptr);
        verify_not_empty_matrices<U, V>(*left_ptr, *right_ptr);
        verify_ntt_prior_to_mult(*left_ptr, *right_ptr);

        auto result = std::make_shared<matrix<seal::Ciphertext>>(left_ptr->rows, right_ptr->cols);

        auto p = std::make_unique<concurrency::promise<matrix<seal::Ciphertext>>>(int(left_ptr->rows * right_ptr->cols),
                                                                                  result);
        auto latch = p->get_latch();
        for (uint64_t i = 0; i < left_ptr->rows; ++i) {
            for (uint64_t j = 0; j < right_ptr->cols; ++j) {
                pool->submit(
                    {
                        .f = [&, i, j, result, left_ptr, right_ptr]() {
                            (*result)(i, j) = mult_row<U, V>(i, j, *left_ptr, *right_ptr);
                        },
                        .wg = latch,
                    }
                );
            }
        }
        return p;
    }

    template<typename U, typename V>
    std::unique_ptr<concurrency::promise<matrix<seal::Ciphertext>>>
    MatrixOperations::async_mat_mult(const std::unique_ptr<matrix<U>> &left,
                                     const std::unique_ptr<matrix<V>> &right) const {


        auto left_ptr = std::make_shared<matrix<U>>(std::move(left));
        auto right_ptr = std::make_shared<matrix<V>>(std::move(right));

        return async_mat_mult(left_ptr, right_ptr);
    }

    // TODO: reuse code with the following function.
    template<typename U>
    std::unique_ptr<concurrency::promise<matrix<seal::Ciphertext>>>
    MatrixOperations::async_scalar_dot_product(const std::shared_ptr<std::vector<std::uint64_t>> &vec,
                                               const std::shared_ptr<matrix<U>> &mat) const {

        auto result_vec = std::make_shared<matrix<seal::Ciphertext>>(1, mat->cols);

        auto p = std::make_unique<concurrency::promise<matrix<seal::Ciphertext>>>(int(mat->cols), result_vec);
        auto latch = p->get_latch();
        for (uint64_t k = 0; k < mat->cols; k++) {
            pool->submit(
                {
                    .f = [&, k, result_vec, vec, mat]() {
                        seal::Ciphertext tmp(w_evaluator->context);
                        seal::Ciphertext rslt;
                        bool first = true;
                        for (uint64_t j = 0; j < mat->rows; j++) {
                            if (first) {
                                w_evaluator->scalar_multiply((*vec)[j], (*mat)(j, k), rslt);
                                first = false;
                            } else {
                                w_evaluator->scalar_multiply((*vec)[j], (*mat)(j, k), tmp);
                                w_evaluator->add(tmp, rslt, rslt);
                            }
                        }
                        (*result_vec)(0, k) = rslt;
                    },
                    .wg = latch,
                    .name = "scalar_dot_product::vec@M"
                }
            );

        }

        return p;
    }

    template<typename U>
    std::unique_ptr<concurrency::promise<matrix<seal::Ciphertext>>>
    MatrixOperations::async_scalar_dot_product(
        const std::shared_ptr<matrix<U>> &mat,
        const std::shared_ptr<std::vector<std::uint64_t>> &vec) const {

        auto result_vec = std::make_shared<matrix<seal::Ciphertext>>(mat->rows, 1);

        auto p = std::make_unique<concurrency::promise<matrix<seal::Ciphertext>>>(int(mat->rows), result_vec);
        auto latch = p->get_latch();
        for (uint64_t k = 0; k < mat->rows; k++) {
            pool->submit(
                {
                    .f= [&, k, result_vec, vec, mat]() {
                        seal::Ciphertext tmp(w_evaluator->context);
                        seal::Ciphertext rslt;
                        bool first = true;
                        for (uint64_t j = 0; j < mat->cols; j++) {
                            if (first) {
                                w_evaluator->scalar_multiply((*vec)[j], (*mat)(k, j), rslt);
                                first = false;
                            } else {
                                w_evaluator->scalar_multiply((*vec)[j], (*mat)(k, j), tmp);
                                w_evaluator->add(tmp, rslt, rslt);
                            }
                        }
                        (*result_vec)(k, 0) = rslt;
                    },
                    .wg = latch,
                    .name = "scalar_dot_product::M@vec"
                }
            );

        }

        return p;
    }

    template<typename U>
    std::shared_ptr<matrix<seal::Ciphertext>>
    MatrixOperations::scalar_dot_product(
        const std::shared_ptr<matrix<U>> &mat,
        const std::shared_ptr<std::vector<std::uint64_t>> &vec) const {
#ifdef DISTRIBICOM_DEBUG
        assert(mat->rows == vec->size());
#endif
        auto result_vec = std::make_shared<matrix<seal::Ciphertext>>(mat->cols, 1);
        for (uint64_t k = 0; k < mat->cols; k++) {
            seal::Ciphertext tmp(w_evaluator->context);
            seal::Ciphertext rslt;
            bool first = true;
            for (uint64_t j = 0; j < mat->rows; j++) {
                if (first) {
                    w_evaluator->scalar_multiply((*vec)[j], (*mat)(j, k), rslt);
                    first = false;
                } else {
                    w_evaluator->scalar_multiply((*vec)[j], (*mat)(j, k), tmp);
                    w_evaluator->add(tmp, rslt, rslt);
                }
            }
            (*result_vec)(k, 0) = rslt;
        }

        return result_vec;
    }


    template<typename U>
    std::shared_ptr<matrix<seal::Ciphertext>>
    MatrixOperations::scalar_dot_product_col_major(
        const std::shared_ptr<matrix<U>> &mat,
        const std::shared_ptr<std::vector<std::uint64_t>> &vec) const {
#ifdef DISTRIBICOM_DEBUG
        assert(mat->cols == vec->size());
#endif
        auto result_vec = std::make_shared<matrix<seal::Ciphertext>>(mat->rows, 1);
        for (uint64_t k = 0; k < mat->rows; k++) {
            seal::Ciphertext tmp(w_evaluator->context);
            seal::Ciphertext rslt;
            bool first = true;
            for (uint64_t j = 0; j < mat->cols; j++) {
                if (first) {
                    w_evaluator->scalar_multiply((*vec)[j], (*mat)(k, j), rslt);
                    first = false;
                } else {
                    w_evaluator->scalar_multiply((*vec)[j], (*mat)(k, j), tmp);
                    w_evaluator->add(tmp, rslt, rslt);
                }
            }
            (*result_vec)(k, 0) = rslt;
        }

        return result_vec;
    }


}