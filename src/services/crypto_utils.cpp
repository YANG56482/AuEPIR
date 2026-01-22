#include "crypto_utils.hpp"
#include <openssl/rsa.h>
#include <openssl/pem.h>
#include <openssl/err.h>
#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/bn.h> // Needed for BIGNUM
#include <sstream>
#include <iomanip>
#include <iostream>
#include <memory>

namespace services {

    // Helper: Convert bytes to Hex string
    static std::string BytesToHex(const unsigned char* data, size_t len) {
        std::stringstream ss;
        ss << std::hex << std::setfill('0');
        for (size_t i = 0; i < len; ++i) {
            ss << std::setw(2) << (int)data[i];
        }
        return ss.str();
    }

    // Helper: Convert Hex string to bytes
    static std::vector<unsigned char> HexToBytes(const std::string& hex) {
        std::vector<unsigned char> bytes;
        for (unsigned int i = 0; i < hex.length(); i += 2) {
            std::string byteString = hex.substr(i, 2);
            unsigned char byte = (unsigned char)strtol(byteString.c_str(), nullptr, 16);
            bytes.push_back(byte);
        }
        return bytes;
    }

    std::pair<std::string, std::string> CryptoUtils::GenerateRSAKeyPair() {
        // [Fixed for OpenSSL 1.1 Compatibility]
        // Use legacy RSA_generate_key_ex instead of EVP_PKEY_CTX set_bits
        
        EVP_PKEY* pkey = EVP_PKEY_new();
        if (!pkey) return {"", ""};

        RSA* rsa = RSA_new();
        BIGNUM* e = BN_new();
        
        if (!rsa || !e) {
             std::cerr << "CryptoUtils: Failed to alloc RSA or BN" << std::endl;
             if(rsa) RSA_free(rsa);
             if(e) BN_free(e);
             EVP_PKEY_free(pkey);
             return {"", ""};
        }

        BN_set_word(e, RSA_F4);

        if (RSA_generate_key_ex(rsa, 2048, e, NULL) != 1) {
             std::cerr << "CryptoUtils: RSA_generate_key_ex failed" << std::endl;
             ERR_print_errors_fp(stderr);
             RSA_free(rsa); // Free RSA as it wasn't assigned yet
             BN_free(e);
             EVP_PKEY_free(pkey);
             return {"", ""};
        }
        
        // EVP_PKEY_assign_RSA takes ownership of the rsa structure
        if (EVP_PKEY_assign_RSA(pkey, rsa) != 1) {
             std::cerr << "CryptoUtils: EVP_PKEY_assign_RSA failed" << std::endl;
             ERR_print_errors_fp(stderr);
             RSA_free(rsa); // Free it manually on fail
             BN_free(e);
             EVP_PKEY_free(pkey);
             return {"", ""};
        }
        
        BN_free(e); // No longer needed

        // Export to PEM
        BIO* pri = BIO_new(BIO_s_mem());
        BIO* pub = BIO_new(BIO_s_mem());

        PEM_write_bio_PrivateKey(pri, pkey, NULL, NULL, 0, NULL, NULL);
        PEM_write_bio_PUBKEY(pub, pkey);

        size_t pri_len = BIO_pending(pri);
        size_t pub_len = BIO_pending(pub);

        std::vector<char> pri_key(pri_len + 1);
        std::vector<char> pub_key(pub_len + 1);

        BIO_read(pri, pri_key.data(), pri_len);
        BIO_read(pub, pub_key.data(), pub_len);

        pri_key[pri_len] = '\0';
        pub_key[pub_len] = '\0';

        // RSA structure is freed when pkey is freed
        EVP_PKEY_free(pkey);
        BIO_free_all(pri);
        BIO_free_all(pub);

        return {std::string(pub_key.data()), std::string(pri_key.data())};
    }

    std::string CryptoUtils::SignMessage(const std::string& private_key_pem, const std::string& message) {
        BIO* bio = BIO_new_mem_buf(private_key_pem.c_str(), -1);
        EVP_PKEY* pkey = PEM_read_bio_PrivateKey(bio, NULL, NULL, NULL);
        BIO_free(bio);

        if (!pkey) return "";

        EVP_MD_CTX* ctx = EVP_MD_CTX_new();
        EVP_SignInit(ctx, EVP_sha256());
        EVP_SignUpdate(ctx, message.c_str(), message.length());

        unsigned int len = 0;
        // Determine length first
        if (EVP_SignFinal(ctx, nullptr, &len, pkey) != 1) {
            EVP_MD_CTX_free(ctx);
            EVP_PKEY_free(pkey);
            return "";
        }

        std::vector<unsigned char> sig(len);
        
        unsigned int actual_len = 0;
        int ret = EVP_SignFinal(ctx, sig.data(), &actual_len, pkey);
        
        EVP_MD_CTX_free(ctx);
        EVP_PKEY_free(pkey);

        if (ret != 1) return "";

        return BytesToHex(sig.data(), actual_len);
    }

    bool CryptoUtils::VerifySignature(const std::string& public_key_pem, const std::string& message, const std::string& signature_hex) {
        BIO* bio = BIO_new_mem_buf(public_key_pem.c_str(), -1);
        EVP_PKEY* pkey = PEM_read_bio_PUBKEY(bio, NULL, NULL, NULL);
        BIO_free(bio);

        if (!pkey) return false;

        EVP_MD_CTX* ctx = EVP_MD_CTX_new();
        EVP_VerifyInit(ctx, EVP_sha256());
        EVP_VerifyUpdate(ctx, message.c_str(), message.length());

        std::vector<unsigned char> sig_bytes = HexToBytes(signature_hex);
        
        int ret = EVP_VerifyFinal(ctx, sig_bytes.data(), sig_bytes.size(), pkey);

        EVP_MD_CTX_free(ctx);
        EVP_PKEY_free(pkey);

        return ret == 1;
    }

    std::string CryptoUtils::SHA256(const std::string& data) {
        unsigned char hash[EVP_MAX_MD_SIZE];
        unsigned int length = 0;

        EVP_MD_CTX* ctx = EVP_MD_CTX_new();
        if (ctx == nullptr) return "";

        if (EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
            EVP_MD_CTX_free(ctx);
            return "";
        }

        if (EVP_DigestUpdate(ctx, data.c_str(), data.length()) != 1) {
            EVP_MD_CTX_free(ctx);
            return "";
        }

        if (EVP_DigestFinal_ex(ctx, hash, &length) != 1) {
            EVP_MD_CTX_free(ctx);
            return "";
        }

        EVP_MD_CTX_free(ctx);
        return BytesToHex(hash, length);
    }

}
