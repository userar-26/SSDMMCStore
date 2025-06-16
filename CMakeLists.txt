cmake_minimum_required(VERSION 3.29)
project(SSDMMCStore C)

include_directories(include)
include_directories(src/common)

add_library(kvstore
        src/key_value_store/kvs.c
        src/key_value_store/kvs_init.c
        src/common/common.c
        src/key_value_store/kvs_internal.c
        src/ssdmmc_sim/ssdmmc_sim_info.c
        src/ssdmmc_sim/ssdmmc_sim.c
        src/key_value_store/kvs_internal_io.c
        src/key_value_store/kvs_metadata.c
        src/key_value_store/kvs_valid.c)

add_executable(main main.c)

target_link_libraries(main kvstore)
