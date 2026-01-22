#include <iostream>
#include <filesystem>
#include "worker.hpp"
#include "marshal/local_storage.hpp"
#include "services/factory.hpp"

bool is_valid_command_line_args(int argc, char *argv[]) {
    if (argc < 3) {
        std::cout << "Usage: " << argv[0] << " <pir_config_file> <num_threads> <num_repetitions>" << std::endl;
        return false;
    }

    if (!std::filesystem::exists(argv[1])) {
        std::cout << "Config file " << argv[1] << " does not exist" << std::endl;
        return false;
    }

    return true;
}

void config_cpu_usage(const distribicom::AppConfigs &cnfgs) {
    if (cnfgs.worker_num_cpus() > 0) {
        concurrency::num_cpus = cnfgs.worker_num_cpus();
        std::cout << "set global num cpus to:" << concurrency::num_cpus << std::endl;
    }
}

distribicom::AppConfigs load_configs(char *const *argv) {
    auto pir_conf_file = argv[1];
    auto num_worker_threads = std::stoi(argv[2]);

    distribicom::Configs temp_pir_configs;
    auto json_str = load_from_file(pir_conf_file);
    google::protobuf::util::JsonStringToMessage(load_from_file(pir_conf_file), &temp_pir_configs);
    auto c = services::configurations::create_app_configs(
            "",
            temp_pir_configs.polynomial_degree(),
            temp_pir_configs.logarithm_plaintext_coefficient(),
            temp_pir_configs.db_rows(),
            temp_pir_configs.db_cols(),
            temp_pir_configs.size_per_element(),
            1,
            10,
            1,
            temp_pir_configs.worker_step_size()
    );
    c.set_worker_num_cpus(num_worker_threads);

    return c;
}

seal::Plaintext random_ptx(seal::EncryptionParameters &encryption_params) {

    seal::Plaintext p(encryption_params.poly_modulus_degree());

    // avoiding encoder usage - to prevent unwanted transformation to the ptx underlying elements
    std::vector<std::uint64_t> v(encryption_params.poly_modulus_degree(), 0);

    seal::Blake2xbPRNGFactory factory;
    auto gen = factory.create();

    auto mod = encryption_params.plain_modulus().value();
    std::generate(v.begin(), v.end(), [gen = std::move(gen), &mod]() { return gen->generate() % mod; });

    for (std::uint64_t i = 0; i < encryption_params.poly_modulus_degree(); ++i) {
        p[i] = v[i];
    }
    return p;
}

seal::Ciphertext random_ctx(seal::EncryptionParameters &enc_params) {
    seal::SEALContext seal_context(enc_params, true);
    seal::KeyGenerator key_gen(seal_context);
    seal::SecretKey secret_key(key_gen.secret_key());
    seal::Encryptor encryptor(seal::Encryptor(seal::SEALContext(seal_context), secret_key));

    auto ptx = random_ptx(enc_params);
    seal::Ciphertext ctx;
    encryptor.encrypt_symmetric(ptx, ctx);

    return ctx;
}

auto msg = "Our DB sizes from the experiment (rowsXcols): 116x116 (~2^19), 164x164 (~2^20), 232x232 (~2^21)";

int main(int argc, char *argv[]) {
    if (!is_valid_command_line_args(argc, argv)) {
        return -1;
    }
    std::cout << msg << std::endl;

    auto appcnfgs = load_configs(argv);
    auto num_repetions = std::stoi(argv[3]);

    config_cpu_usage(appcnfgs);

    seal::EncryptionParameters enc_params = services::utils::setup_enc_params(appcnfgs);

    auto pool = std::make_shared<concurrency::threadpool>();
    auto matops = math_utils::MatrixOperations::Create(
            math_utils::EvaluatorWrapper::Create(enc_params),
            pool
    );

    auto db_cols = appcnfgs.configs().db_cols(), num_queries_per_worker = appcnfgs.configs().db_rows();

    math_utils::matrix<seal::Plaintext> ptx_vector(1, db_cols);
    math_utils::matrix<seal::Ciphertext> ctx_mat(db_cols, num_queries_per_worker,
                                                 random_ctx(enc_params));
    math_utils::matrix<seal::Ciphertext> result_mat(1, num_queries_per_worker);

    matops->to_ntt(ctx_mat.data);

    std::cout << "======" << std::endl;
    std::cout << "db size:                " << db_cols * num_queries_per_worker * 39 << std::endl;
    std::cout << "num queries per worker: " << num_queries_per_worker << std::endl;
    std::cout << "plaintext row:          " << db_cols << std::endl;
    std::cout << "num repetitions:        " << num_repetions << std::endl;
    std::cout << "======" << std::endl;

    std::cout << "starting benchmark:" << std::endl;


    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_repetions; ++i) {
        matops->multiply(ptx_vector, ctx_mat, result_mat);
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "total time: " << double(duration.count()) / double(num_repetions) << " ms" << std::endl;

    return 0;
}
