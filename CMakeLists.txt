cmake_minimum_required(VERSION 3.29)
project(SSDMMCStore C)

set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -fprofile-arcs -ftest-coverage")
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} -fprofile-arcs")

include_directories(include)
include_directories(src/common)

add_library(kvstore
        src/key_value_store/kvs.c
        src/key_value_store/kvs_init.c
        src/key_value_store/kvs_internal.c
        src/ssdmmc_sim/ssdmmc_sim_info.c
        src/ssdmmc_sim/ssdmmc_sim.c
        src/key_value_store/kvs_internal_io.c
        src/key_value_store/kvs_metadata.c
        src/key_value_store/kvs_valid.c)

add_executable(main main.c)

target_link_libraries(main kvstore)
