#pragma once

#include <future>
#include "pir_client.hpp"
#include "db.hpp"
#include "utils.hpp"
#include "distribicom.grpc.pb.h"
#include "distribicom.pb.h"

namespace services {
/**
* ClientInfo stores objects related to an individual client
*/
    struct ClientInfo {
        std::unique_ptr<distribicom::WorkerTaskPart> query_to_send;
        std::unique_ptr<distribicom::WorkerTaskPart> galois_keys_marshaled;

        PirQuery query;
        seal::GaloisKeys galois_keys;
        std::unique_ptr<distribicom::Client::Stub> client_stub;

        std::shared_ptr<math_utils::matrix<seal::Plaintext>> partial_answer;

        PIRClient* local_client_instance = nullptr;

        std::shared_ptr<math_utils::matrix<seal::Ciphertext>> final_answer;
        std::uint64_t answer_count;
    };

    void
    set_client(std::uint32_t expansion_ratio, std::uint32_t db_rows, int client_id,
               const seal::GaloisKeys &gkey, std::string &gkey_serialised, std::vector<std::vector<seal::Ciphertext>> &query,
               const distribicom::ClientQueryRequest &query_marshaled,
               std::unique_ptr<services::ClientInfo> &client_info);



    struct ClientDB {
        std::unique_ptr<std::shared_mutex> mutex=std::make_unique<std::shared_mutex>();
        std::map<uint32_t, std::unique_ptr<ClientInfo>> id_to_info;
        std::uint64_t client_counter = 0;


//        struct shared_cdb {
//            explicit shared_cdb(const ClientDB &db, std::shared_mutex &mtx) : db(db), lck(mtx) {}
//
//            const ClientDB &db;
//        private:
//            std::shared_lock<std::shared_mutex> lck;
//        };
//
//        // gives access to a locked reference of the cdb.
//        // as long as the shared_mat instance is not destroyed and we can't add a client.
//        // useful for distributing the DB, or going over multiple rows.
//        shared_cdb many_reads() {
//            return shared_cdb(*this, *mutex);
//        }


//        std::uint64_t add_client(grpc::ServerContext *context, const distribicom::ClientRegistryRequest *request, const distribicom::Configs& pir_configs, std::uint32_t expansion_ratio){
//            auto requesting_client = utils::extract_ip(context);
//            std::string subscribing_client_address = requesting_client + ":" + std::to_string(request->client_port());
//
//            // creating stub to the client:
//            auto client_conn = std::make_unique<distribicom::Client::Stub>(distribicom::Client::Stub(
//                    grpc::CreateChannel(
//                            subscribing_client_address,
//                            grpc::InsecureChannelCredentials()
//                    )
//            ));
//
//            auto client_info = std::make_unique<ClientInfo>(ClientInfo());
//            client_info->galois_keys_marshaled.set_keys(request->galois_keys());
//            client_info->client_stub = std::move(client_conn);
//
//            std::unique_lock lock(*mutex);
//            client_info->galois_keys_marshaled.set_key_pos(client_counter);
//            client_info->answer_count=0;
//            client_info->partial_answer = std::make_unique<math_utils::matrix<seal::Plaintext>>
//                    (math_utils::matrix<seal::Plaintext>(pir_configs.db_rows(), expansion_ratio));
//            id_to_info.insert(
//                    {client_counter, std::move(client_info)});
//            client_counter += 1;
//            return client_counter-1;
//        };



    };
}

