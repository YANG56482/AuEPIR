
include(FetchContent)


add_subdirectory(concurrency)
add_subdirectory(math_utils)
add_subdirectory(marshal)
add_subdirectory(services)
add_subdirectory(internals)

add_library(distribicom_cpp INTERFACE)


target_include_directories(distribicom_cpp INTERFACE marshal math_utils services ${com_sealpir_SOURCE_DIR}/src ${CMAKE_CURRENT_SOURCE_DIR})

find_package(OpenMP REQUIRED)
target_link_libraries(distribicom_cpp INTERFACE marshal_package multiplication_utils sealpir services OpenMP::OpenMP_CXX)


add_executable(main1 main.cpp)

set_target_properties(
        main1
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/examples
)
#target_include_directories(main1 PUBLIC ${com_sealpir_SOURCE_DIR}/src)
target_link_libraries(main1 distribicom_cpp)

# eval executables:
add_executable(main_server server.cpp)
add_executable(worker worker.cpp)

add_executable(worker_bench worker_benchmark.cpp)

target_link_libraries(main_server distribicom_cpp)
target_link_libraries(worker distribicom_cpp)

target_link_libraries(worker_bench distribicom_cpp)

# FastPIR Integration
# CMAKE_CURRENT_SOURCE_DIR is src/
# We found FastPIR at sibling directory: e:/PIR/FastPIR/src
# Since we are in e:/PIR/DPIR/src, we need to go up two levels to e:/PIR, then FastPIR/src
set(FASTPIR_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../FastPIR/src")

if(EXISTS "${FASTPIR_SRC_DIR}")
    # Remove global include_directories
    # include_directories(${FASTPIR_SRC_DIR})
    
    add_executable(main_fastpir 
        main_fastpir.cpp
        ${FASTPIR_SRC_DIR}/fastpirparams.cpp
        ${FASTPIR_SRC_DIR}/client.cpp
        ${FASTPIR_SRC_DIR}/server.cpp
    )
    
    target_link_libraries(main_fastpir distribicom_cpp)
    target_include_directories(main_fastpir PRIVATE ${FASTPIR_SRC_DIR})
else()
    message(WARNING "FastPIR source not found at ${FASTPIR_SRC_DIR}. Skipping main_fastpir.")
endif()
