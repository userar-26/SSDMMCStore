cmake_minimum_required(VERSION 3.29)
project(SSDMMCStore C)

set(CMAKE_C_STANDARD 23)

include_directories(include)
include_directories(src/common)

add_library(kvstore
        src/key_value_store/kv_store.c
        src/key_value_store/kvs_init.c
        src/common/common.c
        src/key_value_store/kv_store_internal.c)

add_executable(main main.c)

target_link_libraries(main kvstore)
