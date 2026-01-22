
add_library(services worker_strategy.hpp worker_strategy.cpp worker.hpp factory.hpp factory.cpp worker.cpp constants.hpp manager.hpp manager.cpp
        server.hpp server.cpp db.hpp db.cpp utils.hpp utils.cpp client_service.cpp client_service.hpp client_context.hpp client_context.cpp
        authenticator.hpp authenticator.cpp crypto_utils.hpp crypto_utils.cpp)

target_include_directories(services PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(services PRIVATE ${com_sealpir_SOURCE_DIR}/src)

find_package(OpenSSL REQUIRED)
target_link_libraries(services PUBLIC distribicom_grpc boot_grpc SEAL::seal multiplication_utils marshal_package sealpir OpenSSL::SSL OpenSSL::Crypto)