#pragma once

#include <string>
#include <vector>
#include <memory>
#include <random>
#include "distribicom.pb.h"

namespace services {

    /**
     * Authenticator component for Worker Registration (Protocol C. Register).
     * Simulates a trusted hardware module that holds a master secret key (ak).
     */
    class Authenticator {
    public:
        Authenticator();

        /**
         * P. Register: RRes(ak, M_r)
         * Creates a new account binding (sk_ss, pk_ss) and signs credentials.
         * 
         * @param nonce The challenge random number (rs_j)
         * @param server_uid The server's identity (ids)
         * @param worker_uid The worker's identity (id)
         * @return pair of {Credentials, Signature} bytes
         */
        struct RegistrationResult {
            std::string credential_id;
            std::string account_public_key; // pk_ss
            std::string signature;          // sigma
            std::string associated_data;    // ad_j (serialized for verification)
        };

        RegistrationResult register_worker(const std::string& nonce, const std::string& server_uid, const std::string& worker_uid);

        /**
         * D. Authenticate: ARes(id, h(Zj))
         * Signs the computation result hash to prove work was done.
         * 
         * @param server_uid The server's identity (ids)
         * @param computation_hash The hash of Z (h_r)
         * @return pair of {Signature, AssociatedData}
         */
        struct AuthenticationResult {
            std::string signature;       // sigma
            std::string associated_data; // ad_j = (H(id), n)
        };
        
        AuthenticationResult sign_computation(const std::string& server_uid, const std::string& computation_hash);

    private:
        // Authenticator's Private Account Key (RSA) - In real implementation, this is hardware-protected.
        std::string private_key_pem_;
        std::string public_key_pem_;
        
        // Counter n
        uint64_t counter_ = 0;

        std::mt19937_64 rng_;

        // Helper to sign data using "ak"
        std::string sign_data(const std::string& data);
    };

}
