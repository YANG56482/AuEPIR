#include "worker.hpp"
#include "utils.hpp"
#include "distribicom.pb.h"
#include <grpc++/grpc++.h>

namespace services {

    // todo: take a const ref of the app_configs:
    Worker::Worker(distribicom::AppConfigs &&wcnfgs)
        : symmetric_secret_key(), appcnfgs(std::move(wcnfgs)), chan(), mrshl() {
        inspect_configs();
        seal::EncryptionParameters enc_params = utils::setup_enc_params(appcnfgs);

        mrshl = marshal::Marshaller::Create(enc_params);


        // todo: put in a different function.
        auto manager_conn = std::make_unique<distribicom::Manager::Stub>(distribicom::Manager::Stub(
            grpc::CreateChannel(
                appcnfgs.main_server_hostname(),
                grpc::InsecureChannelCredentials()
            )
        ));

        grpc::ClientContext context;
        distribicom::Ack response;
        distribicom::WorkerRegistryRequest request;


        symmetric_secret_key.resize(32);
        std::random_device rand;
        auto gen = seal::Blake2xbPRNGFactory({rand()}).create();
        std::generate(
            symmetric_secret_key.begin(),
            symmetric_secret_key.end(),
            [gen = std::move(gen)]() { return std::byte(gen->generate() % 256); }
        );

        double prob = appcnfgs.configs().malicious_probability();

        strategy = std::make_shared<work_strategy::RowMultiplicationStrategy>(
            enc_params,
            mrshl, std::move(manager_conn), utils::byte_vec_to_64base_string(symmetric_secret_key),
            prob
        );

        threads.emplace_back(
            [&]() {
                std::cout << "worker main thread: running" << std::endl;
                for (;;) {
                    auto task_ = chan.read();
                    if (!task_.ok) {
                        std::cout << "worker main thread: stopping execution" << std::endl;
                        break;
                    }
                    std::cout << "worker main thread: processing task." << std::endl;
                    strategy->process_task(std::move(task_.answer));
                }
            });

        setup_stream();
    }

    void Worker::inspect_configs() const {
        if (appcnfgs.configs().scheme() != "bgv") {
            throw std::invalid_argument("currently not supporting any scheme oother than bgv.");
        }

        auto row_size = appcnfgs.configs().db_cols();
        if (row_size <= 0) {
            throw std::invalid_argument("row size must be positive");
        }
        if (row_size > 10 * 1000 || row_size <= 0) {
            throw std::invalid_argument("row size must be between 1 and 10k");
        }
    }

    void Worker::update_current_task() {
        auto tmp = read_val.matrixpart();
        int row = tmp.row();
        int col = tmp.col();

        if (tmp.has_ptx()) {
            if (!task.ptx_rows.contains(row)) {
                task.ptx_rows[row] = std::move(std::vector<seal::Plaintext>(task.row_size));
            }
            if (col >= task.row_size) {
                throw std::invalid_argument("Invalid column index, too big");
            }
            task.ptx_rows[row][col] = std::move(mrshl->unmarshal_seal_object<seal::Plaintext>(tmp.ptx().data()));
            return;
        }

        if (!tmp.has_ctx()) {
            throw std::invalid_argument("Invalid matrix part, neither ptx nor ctx");
        }
        if (!task.ctx_cols.contains(col)) {
            task.ctx_cols[col] = std::move(std::vector<seal::Ciphertext>(1));
        }
        task.ctx_cols[col][0] = std::move(mrshl->unmarshal_seal_object<seal::Ciphertext>(tmp.ctx().data()));
    }


    void Worker::OnReadDone(bool ok) {
        try {
            if (!ok) {
                // bad read might mean data too big.
                // bad read might mean closed stream.
                // todo: find proper way to find out the error!
                return;
            }

            switch (read_val.part_case()) {
                case distribicom::WorkerTaskPart::PartCase::kGkey:
//                    std::cout << "received galois keys" << std::endl;
                    strategy->store_galois_key(
                        std::move(mrshl->unmarshal_seal_object<seal::GaloisKeys>(read_val.gkey().keys())),
                        int(read_val.gkey().key_pos())
                    );

                    break;

                case distribicom::WorkerTaskPart::PartCase::kMatrixPart:
//                    std::cout << "received matrix part" << std::endl;
                    update_current_task();
                    break;

                case distribicom::WorkerTaskPart::PartCase::kMd:
                    std::cout << "received rnd and epoch" << std::endl;
                    task.round = int(read_val.md().round());
                    task.epoch = int(read_val.md().epoch());
                    break;

                case distribicom::WorkerTaskPart::PartCase::kTaskComplete:
                    std::cout << "done receiving" << std::endl;
                    if (!task.ptx_rows.empty() || !task.ctx_cols.empty()) {
                        chan.write(std::move(task));

                        // TODO: does this leak memory?
                        task = WorkerServiceTask();
                        task.row_size = int(appcnfgs.configs().db_cols());
                    }
                    break;

                default:
                    throw std::invalid_argument("worker stream received unknown message received.");
            }

            read_val.clear_part();
            read_val.clear_gkey();
            read_val.clear_matrixpart();

        } catch (std::exception &e) {
            std::cerr << "Worker::OnReadDone: failure: " << e.what() << std::endl;
        }

        StartRead(&read_val);// queue the next read request.
    }

    void Worker::OnDone(const grpc::Status &s) {
        std::cout << "Closing worker's stream!" << std::endl;
        std::unique_lock<std::mutex> l(mu_);
        status_ = s;
        done_ = true;
        cv_.notify_all();
    }

    grpc::Status Worker::wait_for_stream_termination() {
        std::unique_lock<std::mutex> l(mu_);
        cv_.wait(l, [this] { return done_; });
        return status_;
    }

    void Worker::close() {
        chan.close();
        for (auto &t: threads) {
            t.join();
        }
    }

    void Worker::setup_stream() {
        // TODO: setup any value that we need in our stream here:

        task.row_size = int(appcnfgs.configs().db_cols());

        // write worker's credentials here:


        threads.emplace_back([&]() {
            grpc::ChannelArguments ch_args;
            ch_args.SetMaxReceiveMessageSize(constants::max_message_size);
            ch_args.SetMaxSendMessageSize(constants::max_message_size);
            this->stub = distribicom::Manager::NewStub(
                grpc::CreateCustomChannel(
                    appcnfgs.main_server_hostname(),
                    grpc::InsecureChannelCredentials(),
                    ch_args
                )
            );

            distribicom::WorkerRegistryRequest rqst;
            utils::add_metadata_string(context_, constants::credentials_md,
                                       utils::byte_vec_to_64base_string(symmetric_secret_key));
            this->stub->async()->RegisterAsWorker(&context_, &rqst, this);

            StartRead(&read_val); // queueing a read request.
            StartCall();

            auto status = wait_for_stream_termination();

            if (!status.ok()) {
                std::cout << "Worker stream terminated with error: " << status.error_message() << std::endl;
                return;
            }
            std::cout << "Worker stream terminated successfully." << std::endl;
        });
    }

}

