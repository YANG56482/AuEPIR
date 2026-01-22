#include "evaluator_wrapper.hpp"
#include <seal/util/polyarithsmallmod.h>

namespace math_utils {

    /*
     * for the correct expansion ratio take last_parms_id() times 2
     */
    uint32_t compute_expansion_ratio(const seal::EncryptionParameters &params) {
        uint32_t expansion_ratio = 0;
        uint32_t pt_bits_per_coeff = log2(params.plain_modulus().value());
        for (size_t i = 0; i < params.coeff_modulus().size(); ++i) {
            double coeff_bit_size = log2(params.coeff_modulus()[i].value());
            expansion_ratio += ceil(coeff_bit_size / pt_bits_per_coeff);
        }
        return expansion_ratio;
    }
}
namespace {
    uint32_t compute_expansion_ratio(const seal::EncryptionParameters &params) {
        uint32_t expansion_ratio = 0;
        uint32_t pt_bits_per_coeff = log2(params.plain_modulus().value());
        for (size_t i = 0; i < params.coeff_modulus().size(); ++i) {
            double coeff_bit_size = log2(params.coeff_modulus()[i].value());
            expansion_ratio += ceil(coeff_bit_size / pt_bits_per_coeff);
        }
        return expansion_ratio;
    }

    std::vector<seal::Plaintext> decompose_to_plaintexts(const seal::EncryptionParameters &params,
                                                         const seal::Ciphertext &ct) {
        const uint32_t pt_bits_per_coeff = log2(params.plain_modulus().value());
        const auto coeff_count = params.poly_modulus_degree();
        const auto coeff_mod_count = params.coeff_modulus().size();
        const uint64_t pt_bitmask = (1 << pt_bits_per_coeff) - 1;

        std::vector<seal::Plaintext> result(compute_expansion_ratio(params) * ct.size());
        auto pt_iter = result.begin();
        for (size_t poly_index = 0; poly_index < ct.size(); ++poly_index) {
            for (size_t coeff_mod_index = 0; coeff_mod_index < coeff_mod_count;
                 ++coeff_mod_index) {
                const double coeff_bit_size =
                        log2(params.coeff_modulus()[coeff_mod_index].value());
                const size_t local_expansion_ratio =
                        ceil(coeff_bit_size / pt_bits_per_coeff);
                size_t shift = 0;
                for (size_t i = 0; i < local_expansion_ratio; ++i) {
                    pt_iter->resize(coeff_count);
                    for (size_t c = 0; c < coeff_count; ++c) {
                        (*pt_iter)[c] =
                                (ct.data(poly_index)[coeff_mod_index * coeff_count + c] >>
                                                                                        shift) &
                                pt_bitmask;
                    }
                    ++pt_iter;
                    shift += pt_bits_per_coeff;
                }
            }
        }
        return result;
    }

    void compose_to_ciphertext(const seal::EncryptionParameters &params,
                               std::vector<seal::Plaintext>::const_iterator pt_iter,
                               const size_t ct_poly_count, seal::Ciphertext &ct) {
        const uint32_t pt_bits_per_coeff = log2(params.plain_modulus().value());
        const auto coeff_count = params.poly_modulus_degree();
        const auto coeff_mod_count = params.coeff_modulus().size();

        ct.resize(ct_poly_count);
        for (size_t poly_index = 0; poly_index < ct_poly_count; ++poly_index) {
            for (size_t coeff_mod_index = 0; coeff_mod_index < coeff_mod_count;
                 ++coeff_mod_index) {
                const double coeff_bit_size =
                        log2(params.coeff_modulus()[coeff_mod_index].value());
                const size_t local_expansion_ratio =
                        ceil(coeff_bit_size / pt_bits_per_coeff);
                size_t shift = 0;
                for (size_t i = 0; i < local_expansion_ratio; ++i) {
                    for (size_t c = 0; c < pt_iter->coeff_count(); ++c) {
                        if (shift == 0) {
                            ct.data(poly_index)[coeff_mod_index * coeff_count + c] =
                                    (*pt_iter)[c];
                        } else {
                            ct.data(poly_index)[coeff_mod_index * coeff_count + c] +=
                                    ((*pt_iter)[c] << shift);
                        }
                    }
                    ++pt_iter;
                    shift += pt_bits_per_coeff;
                }
            }
        }
    }

    void compose_to_ciphertext(const seal::EncryptionParameters &params,
                               const std::vector<seal::Plaintext> &pts, seal::Ciphertext &ct) {
        return compose_to_ciphertext(
                params, pts.begin(), pts.size() / compute_expansion_ratio(params), ct);
    }
}

namespace math_utils {

    void EvaluatorWrapper::add_plain(const seal::Plaintext &a, const seal::Plaintext &b,
                                     seal::Plaintext &c) const {
        seal::Plaintext tmp;
        tmp = a;

        add_plain_in_place(tmp, b);
        c = tmp;
    }

    void EvaluatorWrapper::add_plain_trivial(const seal::Plaintext &a, const seal::Plaintext &b,
                                             seal::Ciphertext &out) const {
        seal::Ciphertext tmp_a, tmp_b;
        trivial_ciphertext(a, tmp_a);
        trivial_ciphertext(b, tmp_b);
        evaluator->add(tmp_a, tmp_b, out);
    }


    void EvaluatorWrapper::add_plain_in_place(seal::Plaintext &a, const seal::Plaintext &b) const {
        auto ptx_mod = enc_params.plain_modulus().value();

        auto coeff_count = a.coeff_count();
        for (std::uint64_t current_coeff = 0; current_coeff < coeff_count; current_coeff++) {
            a[current_coeff] += b[current_coeff];
            if (a[current_coeff] >= ptx_mod) {
                a[current_coeff] -= ptx_mod;
            }
        }
    }

    void EvaluatorWrapper::mult_slow(const seal::Plaintext &a, const seal::Ciphertext &b,
                                     seal::Ciphertext &c) const {
        evaluator->add_plain(trivial_zero, a, c);
        evaluator->multiply_inplace(c, b);
    }

    /**
     * The resulted SplitPlaintextNTTForm is in NTT.
     */
    SplitPlaintextNTTForm EvaluatorWrapper::split_plaintext(const seal::Plaintext &a) const {
#ifdef DISTRIBICOM_DEBUG
        assert(!a.is_ntt_form());
#endif

        seal::Plaintext a1(enc_params.poly_modulus_degree());
        seal::Plaintext a2(enc_params.poly_modulus_degree());

        auto threshold = (enc_params.plain_modulus().value() + 1) >> 1;

        auto coeff_count = a.coeff_count();
        for (uint32_t current_coeff = 0; current_coeff < coeff_count; current_coeff++) {
            // plain modulus is odd, thus threshold is an even number.
            if (a[current_coeff] >= threshold) {
                // skipping if to perform + 1 in case a[current_coeff] is odd.
                a1[current_coeff] = (a[current_coeff] >> 1) + (a[current_coeff] & 1);
                a2[current_coeff] = (a[current_coeff] >> 1);
            } else {
                a1[current_coeff] = a[current_coeff];
                a2[current_coeff] = 0;
            }
        }

#ifdef DISTRIBICOM_DEBUG
        assert_plain_is_under_threshold(a1);
        assert_plain_is_under_threshold(a2);
#endif

        evaluator->transform_to_ntt_inplace(a1, context.first_parms_id());
        evaluator->transform_to_ntt_inplace(a2, context.first_parms_id());

        return {
                std::move(a1),
                std::move(a2),
        };
    }

    void
    EvaluatorWrapper::mult(const SplitPlaintextNTTForm &a, const seal::Ciphertext &b,
                           seal::Ciphertext &c) const {
#ifdef DISTRIBICOM_DEBUG
        assert(2 == a.size());
        for (auto &a_i: a) {
            assert(a_i.is_ntt_form());
        }
        assert(b.is_ntt_form());
#endif
        evaluator->multiply_plain(b, a[0], c);

        seal::Ciphertext tmp;
        evaluator->multiply_plain(b, a[1], tmp);

        evaluator->add(c, tmp, c);
    }

    void
    EvaluatorWrapper::mult(const seal::Plaintext &a, const seal::Ciphertext &b, seal::Ciphertext &c) const {
#ifdef DISTRIBICOM_DEBUG
        assert(!a.is_ntt_form());
        assert(b.is_ntt_form());
#endif
        auto split_ptx = split_plaintext(a);
        mult(split_ptx, b, c);
    }

    void EvaluatorWrapper::mult_reg(const seal::Plaintext &a, const seal::Ciphertext &b, seal::Ciphertext &c) const {
#ifdef DISTRIBICOM_DEBUG
        assert(a.is_ntt_form());
        assert(b.is_ntt_form());
#endif

        evaluator->multiply_plain(b, a, c);
    }

    void
    EvaluatorWrapper::mult(const seal::Ciphertext &a, const seal::Ciphertext &b, seal::Ciphertext &c) const {
#ifdef DISTRIBICOM_DEBUG
        assert(!a.is_ntt_form());
        assert(!b.is_ntt_form());
#endif

        evaluator->multiply(a, b, c);
    }

    std::shared_ptr<EvaluatorWrapper> EvaluatorWrapper::Create(seal::EncryptionParameters enc_params) {
        auto eval = EvaluatorWrapper(enc_params);
        return std::make_shared<EvaluatorWrapper>(std::move(eval));
    }

    EvaluatorWrapper::EvaluatorWrapper(seal::EncryptionParameters enc_params) :
            enc_params(enc_params),
            context(enc_params, true) {

        evaluator = std::make_unique<seal::Evaluator>(context);
        trivial_zero = seal::Ciphertext(context);
        trivial_zero.resize(2);
    }

    void EvaluatorWrapper::trivial_ciphertext(const seal::Plaintext &ptx, seal::Ciphertext &result) const {
        evaluator->add_plain(trivial_zero, ptx, result);
    }

    void EvaluatorWrapper::assert_plain_is_under_threshold(const seal::Plaintext &plaintext) const {
        auto threshold = (enc_params.plain_modulus().value() + 1) >> 1;
        auto coeff_count = plaintext.coeff_count();
        for (uint32_t current_coeff = 0; current_coeff < coeff_count; current_coeff++) {
            assert(plaintext[current_coeff] < threshold);
        }
    }

    void EvaluatorWrapper::get_ptx_embedding(const seal::Ciphertext &ctx, EmbeddedCiphertext &ptx_decomposition) const {
        auto context_data = context.get_context_data(ctx.parms_id());
        if (!context_data) {
             // Fallback or error? If ctx is invalid, this is bad.
             // Try first context? No, that causes the crash.
             // Throw?
             throw std::invalid_argument("EvaluatorWrapper::get_ptx_embedding: Invalid CTX parms_id");
        }
        ptx_decomposition = std::move(decompose_to_plaintexts(context_data->parms(), ctx));
    }

    void EvaluatorWrapper::compose_to_ctx(const std::vector<seal::Plaintext> &ptx_decomposition,
                                          seal::Ciphertext &decoded) const {
        seal::Ciphertext ctx_copy(context, context.first_parms_id());
        compose_to_ciphertext(context.first_context_data()->parms(), ptx_decomposition, ctx_copy);
        decoded = std::move(ctx_copy);
    }


    void EvaluatorWrapper::mult_with_ptx_decomposition(const EmbeddedCiphertext &ptx_decomposition,
                                                       const seal::Ciphertext &ctx,
                                                       EncryptedEmbeddedCiphertext &result) const {
#ifdef DISTRIBICOM_DEBUG
        assert(ptx_decomposition[0].is_ntt_form());
        assert(ctx.is_ntt_form());
#endif

        std::vector<seal::Ciphertext> result_copy(ptx_decomposition.size());
        for (auto i = 0; i < ptx_decomposition.size(); i++) {
            evaluator->multiply_plain(ctx, ptx_decomposition[i], result_copy[i]);
        }
        result = std::move(result_copy);
    }

    void EvaluatorWrapper::scalar_multiply(std::uint64_t scalar, const seal::Ciphertext &right,
                                           seal::Ciphertext &sum) const {
        sum.resize(context, right.parms_id(), right.size());
        sum.is_ntt_form() = right.is_ntt_form();
        sum.scale() = right.scale();

//        seal::Plaintext ptx;
//        ptx = scalar;
//        evaluator->multiply_plain(right, ptx, sum);

        auto &ctx_data = *context.get_context_data(sum.parms_id());
        auto &params = ctx_data.parms();

        seal::util::negacyclic_multiply_poly_mono_coeffmod(
                right, right.size(),
                scalar,
                0 /*dominant exponent is 0 (polynomial is scalar)*/,
                params.coeff_modulus() /* modulus iter*/,
                sum /* result put here*/,
                seal::MemoryManager::GetPool()
        );
    }


    void EvaluatorWrapper::transform_to_ntt_inplace(EmbeddedCiphertext &encoded) const {
        for (auto &ptx_piece: encoded) {
            evaluator->transform_to_ntt_inplace(ptx_piece, context.first_parms_id());
        }
    }


    void EvaluatorWrapper::add_embedded_ctxs(const EncryptedEmbeddedCiphertext &ctx_decomposition1,
                                             const EncryptedEmbeddedCiphertext &ctx_decomposition2,
                                             EncryptedEmbeddedCiphertext &result) const {
#ifdef DISTRIBICOM_DEBUG
        assert(ctx_decomposition1[0].is_ntt_form());
        assert(ctx_decomposition2[0].is_ntt_form());
        assert(ctx_decomposition1.size() == ctx_decomposition2.size());
#endif

        EncryptedEmbeddedCiphertext result_copy(ctx_decomposition1.size());
        for (auto i = 0; i < ctx_decomposition1.size(); i++) {
            evaluator->add(ctx_decomposition1[i], ctx_decomposition2[i], result_copy[i]);
        }
        result = std::move(result_copy);
    }

}
