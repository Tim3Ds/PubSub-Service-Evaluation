cmake_minimum_required(VERSION 3.10)
project(redis_tests)

# Use centralized protobuf
set(CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH};/home/tim/repos/utils/protobuf/build")
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

set(CMAKE_CXX_STANDARD 17)
find_package(Threads REQUIRED)

# Get repo root using git
execute_process(
    COMMAND git rev-parse --show-toplevel
    OUTPUT_VARIABLE REPO_ROOT
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Set path to grpc's build deps where protobuf is installed
set(GRPC_DEPS_PATH "${REPO_ROOT}/grpc/cpp/build/deps")
set(CMAKE_PREFIX_PATH ${GRPC_DEPS_PATH} ${CMAKE_PREFIX_PATH})

# Find Hiredis
find_path(HIREDIS_INCLUDE_DIR hiredis/hiredis.h PATHS ${CMAKE_PREFIX_PATH}/include)
find_library(HIREDIS_LIBRARY hiredis PATHS ${CMAKE_PREFIX_PATH}/lib)

# Find Protobuf
find_package(Protobuf REQUIRED HINTS ${GRPC_DEPS_PATH})

include_directories(${HIREDIS_INCLUDE_DIR})
include_directories(${REPO_ROOT}/utils)
include_directories(${GRPC_DEPS_PATH}/include)
include_directories(${REPO_ROOT}/grpc/cpp/build)
include_directories(${REPO_ROOT}/utils/cpp)


# Generic Protobuf source and Utility sources
set(PROTO_SRC "${REPO_ROOT}/utils/cpp/messaging.pb.cc")
set(UTILS_SRCS "${REPO_ROOT}/utils/cpp/test_data_loader.cpp")

add_executable(sender_test sender_test.cpp ${PROTO_SRC} ${UTILS_SRCS})
target_link_libraries(sender_test ${HIREDIS_LIBRARY} protobuf::libprotobuf stdc++fs)

add_executable(receiver_test receiver_test.cpp ${PROTO_SRC})
target_link_libraries(receiver_test ${HIREDIS_LIBRARY} protobuf::libprotobuf stdc++fs)

add_executable(sender_async_test sender_async_test.cpp ${PROTO_SRC} ${UTILS_SRCS})
target_link_libraries(sender_async_test ${HIREDIS_LIBRARY} Threads::Threads protobuf::libprotobuf stdc++fs)

add_executable(receiver_async_test receiver_async_test.cpp ${PROTO_SRC})
target_link_libraries(receiver_async_test ${HIREDIS_LIBRARY} Threads::Threads protobuf::libprotobuf stdc++fs)
