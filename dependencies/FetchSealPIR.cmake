
include(FetchContent)
FetchContent_Declare(
        com_sealpir
        GIT_REPOSITORY https://github.com/elkanatovey/sealpir-distribicom.git
        GIT_TAG        370be4d69ba242b28905f9da2f41ee667a13a456 #distribicom sealpir version
        USES_TERMINAL_DOWNLOAD ON
        UPDATE_DISCONNECTED ON
)

FetchContent_GetProperties(com_sealpir)

if(NOT com_sealpir_POPULATED)
    FetchContent_Populate(com_sealpir)
endif()

# PATCH: ALWAYS overwrite files with optimized versions (Force Update)
message(STATUS "CHECKPOINT: Patching SealPIR with local optimizations (Force)...")
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/dependencies/patches/pir_server.cpp DESTINATION ${com_sealpir_SOURCE_DIR}/src)
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/dependencies/patches/src_CMakeLists.txt DESTINATION ${com_sealpir_SOURCE_DIR}/src)
file(RENAME ${com_sealpir_SOURCE_DIR}/src/src_CMakeLists.txt ${com_sealpir_SOURCE_DIR}/src/CMakeLists.txt)

add_subdirectory(
        ${com_sealpir_SOURCE_DIR}
        ${com_sealpir_BINARY_DIR}
        EXCLUDE_FROM_ALL)
