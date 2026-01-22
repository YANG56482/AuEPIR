#include "matrix_operations.hpp"
#include "defines.h"
#include <execution>
#include <latch>

namespace math_utils {
    // helper funcs:
    void verify_ptx_ctx_mat_mul_args(const matrix<seal::Plaintext> &left, const matrix<seal::Ciphertext> &right) {
        verify_not_empty_matrices(left, right);
        if (left.data[0].is_ntt_form()) {
            throw std::invalid_argument("MatrixOperations::multiply: left matrix should not be in NTT form");
        }
        if (!right.data[0].is_ntt_form()) {
            throw std::invalid_argument("MatrixOperations::multiply: right matrix should be in NTT form");
        }
    }

    void
    verify_spltptx_ctx_mat_mul_args(const matrix<SplitPlaintextNTTForm> &left, const matrix<seal::Ciphertext> &right) {
        verify_not_empty_matrices(left, right);
        if (!left.data[0][0].is_ntt_form()) {
            throw std::invalid_argument("MatrixOperations::multiply: left matrix should be in NTT form");
        }
        if (!right.data[0].is_ntt_form()) {
            throw std::invalid_argument("MatrixOperations::multiply: right matrix should be in NTT form");
        }
    }

    // actual code:
    void MatrixOperations::transform(std::vector<seal::Plaintext> v, SplitPlaintextNTTFormMatrix &m) const {
        m.resize(v.size());
        std::transform(std::execution::par_unseq,
                       v.begin(),
                       v.end(),
                       m.begin(),
                       [&](seal::Plaintext &ptx) { return w_evaluator->split_plaintext(ptx); }
        );
    }

    void MatrixOperations::transform(std::vector<seal::Plaintext> v, CiphertextDefaultFormMatrix &m) const {
        m.resize(v.size());
        // TODO: into a parallel for loop.
        for (std::uint64_t i = 0; i < v.size(); i++) {
            w_evaluator->trivial_ciphertext(v[i], m[i]);
        }
    }

    void MatrixOperations::transform(const matrix<seal::Plaintext> &mat, matrix<seal::Ciphertext> &cmat) const {
        cmat.resize(mat.rows, mat.cols);
        for (uint64_t i = 0; i < mat.rows * mat.cols; i++) {
            w_evaluator->trivial_ciphertext(mat.data[i], cmat.data[i]);
        }
    }

    // only works when left_vec is 0/1..
    void MatrixOperations::left_multiply(const std::vector<std::uint64_t> &left_vec,
                                         const matrix<seal::Ciphertext> &mat,
                                         matrix<seal::Ciphertext> &result) const {
#ifdef DISTRIBICOM_DEBUG
        assert(left_vec.size() == mat.rows);
        for (auto item: left_vec) {
            assert(item <= 1);
        }
#endif
        result.resize(1, mat.cols);

        for (uint64_t k = 0; k < mat.cols; k++) {
            bool first = true;
            for (uint64_t j = 0; j < mat.rows; j++) {
                if (left_vec[j] == 0) {
                    continue;
                }
                if (first) {
                    result(0, k) = mat(j, k);
                    first = false;
                } else {
                    w_evaluator->add(mat(j, k), result(0, k), result(0, k));
                }
            }
            if (first) {
                // If the vector was all zeros, we need a zero ciphertext of the correct size/params.
                // We use scalar_multiply by 0 on the first element to generate it.
                // Assumes mat.rows > 0 (checked at start of function/class).
                w_evaluator->scalar_multiply(0, mat(0, k), result(0, k));
            }
        }
    }

    std::shared_ptr<MatrixOperations>
    MatrixOperations::Create(std::shared_ptr<EvaluatorWrapper> w_evaluator) {
        auto matops = std::make_shared<MatrixOperations>(w_evaluator);
        return matops;
    }

    std::shared_ptr<MatrixOperations>
    MatrixOperations::Create(std::shared_ptr<EvaluatorWrapper> w_evaluator,
                             std::shared_ptr<concurrency::threadpool> &pool) {
        auto matops = std::make_shared<MatrixOperations>(w_evaluator, pool);
        return matops;
    }

    void MatrixOperations::right_multiply(std::vector<std::uint64_t> &dims, std::vector<SplitPlaintextNTTForm> &matrix,
                                          std::vector<seal::Ciphertext> &right_vec, std::vector<seal::Ciphertext>
                                          &result) {
#ifdef DISTRIBICOM_DEBUG
        // everything needs to be in NTT!
        for (auto &ptx: matrix) {
            assert(ptx[0].is_ntt_form());
        }

        assert(right_vec.size() == dims[COL]);
#endif

        seal::Ciphertext tmp;
        for (uint64_t j = 0; j < dims[ROW]; j++) {
            w_evaluator->mult(matrix[j], right_vec[0], result[j]);
            for (uint64_t k = 1; k < dims[COL]; k++) {

                w_evaluator->mult(
                        matrix[j + k * dims[ROW]],
                        right_vec[k],
                        tmp
                );

                w_evaluator->evaluator->add_inplace(result[j], tmp);

            }
        }
    }

    void MatrixOperations::right_multiply(std::vector<std::uint64_t> &dims, std::vector<seal::Ciphertext> &matrix,
                                          std::vector<seal::Ciphertext> &right_vec, std::vector<seal::Ciphertext>
                                          &result) {
#ifdef DISTRIBICOM_DEBUG
        // everything needs to be in NTT!
        for (auto &ctx: matrix) {
            assert(!ctx.is_ntt_form());
        }
        assert(right_vec.size() == dims[COL]);
#endif
        seal::Ciphertext tmp;
        for (uint64_t j = 0; j < dims[ROW]; j++) {
            w_evaluator->mult(matrix[j], right_vec[0], result[j]);
            for (uint64_t k = 1; k < dims[COL]; k++) {

                w_evaluator->mult(
                        matrix[j + k * dims[ROW]],
                        right_vec[k],
                        tmp
                );

                w_evaluator->evaluator->add_inplace(result[j], tmp);

            }
        }
    }

    void MatrixOperations::to_ntt(std::vector<seal::Plaintext> &m) const {
        std::for_each(std::execution::seq, m.begin(), m.end(), [this](seal::Plaintext &ptx) {
            w_evaluator->evaluator->transform_to_ntt_inplace(ptx, this->w_evaluator->context.first_parms_id());
        });
    }


    void MatrixOperations::to_ntt(std::vector<seal::Ciphertext> &m) const {
        std::for_each(std::execution::seq, m.begin(), m.end(), [this](seal::Ciphertext &ctx) {
            w_evaluator->evaluator->transform_to_ntt_inplace(ctx);
        });
    }

    void MatrixOperations::from_ntt(std::vector<seal::Ciphertext> &m) const {
        std::for_each(std::execution::seq, m.begin(), m.end(), [this](seal::Ciphertext &ctx) {
            w_evaluator->evaluator->transform_from_ntt_inplace(ctx);
        });
    }



    void MatrixOperations::multiply(const matrix<seal::Ciphertext> &left,
                                    const matrix<seal::Ciphertext> &right,
                                    matrix<seal::Ciphertext> &result) const {

        if (left.data[0].is_ntt_form() || right.data[0].is_ntt_form()) {
            throw std::runtime_error("MatrixOperations::multiply: matrices should not be in NTT form");
        }

        seal::Ciphertext tmp;
        result.resize(left.rows, right.cols);
        mat_mult(left, right, result);
    }

    void MatrixOperations::multiply(const matrix<seal::Plaintext> &left,
                                    const matrix<seal::Ciphertext> &right,
                                    matrix<seal::Ciphertext> &result) const {
        verify_ptx_ctx_mat_mul_args(left, right);

        seal::Ciphertext tmp;
        result.resize(left.rows, right.cols);
        matrix<SplitPlaintextNTTForm> left_ntt(left.rows, left.cols);
        transform(left.data, left_ntt.data);

        multiply(left_ntt, right, result);
    }

    void MatrixOperations::multiply(const matrix<SplitPlaintextNTTForm> &left_ntt,
                                    const matrix<seal::Ciphertext> &right,
                                    matrix<seal::Ciphertext> &result) const {

        verify_spltptx_ctx_mat_mul_args(left_ntt, right);
        result.resize(left_ntt.rows, right.cols); // FIX: Ensure result matrix is sized correctly
        mat_mult(left_ntt, right, result);
    }

    void fill_rand_vec(uint64_t size, std::vector<std::uint64_t> &randvec) {
        randvec.resize(size);
        // using random seed:
        seal::Blake2xbPRNGFactory factory;
        auto gen = factory.create();
        for (auto &i: randvec) {
            i = gen->generate() % 2;
        }
    }

    void MatrixOperations::left_frievalds(const std::vector<std::uint64_t> &rand_vec,
                                          const matrix<seal::Ciphertext> &a,
                                          const matrix<seal::Ciphertext> &b,
                                          matrix<seal::Ciphertext> &result) const {
        matrix<seal::Ciphertext> tmp;
        left_multiply(rand_vec, a, tmp);
        multiply(tmp, b, result);
    }

    bool MatrixOperations::frievalds(const matrix<seal::Ciphertext> &a,
                                     const matrix<seal::Ciphertext> &b,
                                     const matrix<seal::Ciphertext> &c) const {
        // nxm * mxp = nxp
        if (a.rows != c.rows || b.cols != c.cols) {
            return false;
        }
        std::vector<std::uint64_t> rand_vec;
        fill_rand_vec(a.rows, rand_vec);

        matrix<seal::Ciphertext> left_vec;
        left_frievalds(rand_vec, a, b, left_vec);

        matrix<seal::Ciphertext> right_vec;
        left_multiply(rand_vec, c, right_vec);

        // subtracting the two vectors:
        for (std::uint64_t i = 0; i < left_vec.data.size(); i++) {
            w_evaluator->evaluator->sub_inplace(left_vec.data[i], right_vec.data[i]);
            if (!left_vec.data[i].is_transparent()) {
                return false;
            }
        }

        return true;
    }


}