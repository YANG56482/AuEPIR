#include "authenticator.hpp"
#include "crypto_utils.hpp"
#include <sstream>
#include <iomanip>
#include <functional> /* for std::hash */

namespace services {

    Authenticator::Authenticator() {
        // Initialize simple RNG
        std::random_device rd;
        rng_.seed(rd());
        
        // REAL CRYPTO: Generate RSA Key Pair for the Account
        auto keys = CryptoUtils::GenerateRSAKeyPair();
        if (keys.first.empty()) {
            throw std::runtime_error("Failed to generate RSA keys for Authenticator");
        }
        public_key_pem_ = keys.first;
        private_key_pem_ = keys.second;
    }

    Authenticator::RegistrationResult Authenticator::register_worker(const std::string& nonce, const std::string& server_uid, const std::string& worker_uid) {
        // 1. In real world, we might generate a NEW key pair here for 'Session' or 'Account binding'
        // But for simplicity, we use the Authenticator's main key as the Account Key.
        
        // 2. Generate Credential ID (cid)
        uint64_t cid_val = rng_();
        std::string cid = std::to_string(cid_val);

        // 3. Construct Associated Data (ad_j)
        // ad_j = (worker_id, nonce, server_id, cid)
        std::stringstream ad_ss;
        ad_ss << worker_uid << "|" << nonce << "|" << server_uid << "|" << cid; 
        std::string ad_j = ad_ss.str();

        // 4. Sign (sigma = Sign(sk, ad_j))
        std::string signature = sign_data(ad_j);

        return RegistrationResult{
            .credential_id = cid,
            .account_public_key = public_key_pem_, // Send Public Key to Server!
            .signature = signature,
            .associated_data = ad_j
        };
    }

    Authenticator::AuthenticationResult Authenticator::sign_computation(const std::string& server_uid, const std::string& computation_hash) {
        // 1. Construct Associated Data (ad_j)
        std::stringstream ad_ss;
        ad_ss << server_uid << "|" << counter_;
        std::string ad_j = ad_ss.str();

        // 2. Sign (sigma = Sign(sk, (ad_j | h_r)))
        std::string combo = ad_j + "|" + computation_hash;
        std::string signature = sign_data(combo);

        // Increment counter
        counter_++;

        return AuthenticationResult{
            .signature = signature,
            .associated_data = ad_j
        };
    }

    std::string Authenticator::sign_data(const std::string& data) {
        // REAL CRYPTO
        return CryptoUtils::SignMessage(private_key_pem_, data);
    }

}
