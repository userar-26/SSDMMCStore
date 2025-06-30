cmake_minimum_required(VERSION 3.29)
project(SSDMMCStore C)

include_directories(include)
include_directories(src/common)
include_directories(tests)

add_library(kvstore
        src/key_value_store/kvs.c
        src/key_value_store/kvs_init.c
        src/key_value_store/kvs_internal.c
        src/ssdmmc_sim/ssdmmc_sim_info.c
        src/ssdmmc_sim/ssdmmc_sim.c
        src/key_value_store/kvs_internal_io.c
        src/key_value_store/kvs_metadata.c
        src/key_value_store/kvs_valid.c)

add_executable(main main.c
        tests/kvs_test_wrappers.c
        tests/kvs_test_wrappers.h)

target_link_libraries(main kvstore)
