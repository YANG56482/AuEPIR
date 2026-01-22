#pragma once

#include <string>
#include <vector>
#include <utility>

namespace services {
    class CryptoUtils {
    public:
        // Generates RSA Key Pair (Public, Private) in PEM format
        static std::pair<std::string, std::string> GenerateRSAKeyPair();

        // Signs data using Private Key (SHA256 digest)
        // Returns Base64 encoded signature or Hex string
        static std::string SignMessage(const std::string& private_key_pem, const std::string& message);

        // Verifies signature using Public Key
        static bool VerifySignature(const std::string& public_key_pem, const std::string& message, const std::string& signature_hex);

        // Compute SHA256 Hash of data (returns Hex string)
        static std::string SHA256(const std::string& data);
    };
}
