cmake_minimum_required(VERSION 3.10)
project(rabbitmq_examples)

# Use centralized protobuf
set(CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH};/home/tim/repos/utils/protobuf/build")
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

set(CMAKE_CXX_STANDARD 17)
find_package(Threads REQUIRED)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
# Get repo root using git
execute_process(
    COMMAND git rev-parse --show-toplevel
    OUTPUT_VARIABLE REPO_ROOT
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
set(PROTO_SRC "${REPO_ROOT}/utils/cpp/messaging.pb.cc")
set(UTILS_SRCS "${REPO_ROOT}/utils/cpp/test_data_loader.cpp")
include_directories(${REPO_ROOT}/utils/cpp)
include_directories(${REPO_ROOT}/utils)


# Find rabbitmq-c using the local build prefix (relies on CMAKE_PREFIX_PATH)
find_path(RABBITMQ_INCLUDE_DIR amqp.h)
find_library(RABBITMQ_LIBRARY NAMES rabbitmq)

if (NOT RABBITMQ_INCLUDE_DIR OR NOT RABBITMQ_LIBRARY)
    message(FATAL_ERROR "rabbitmq-c not found. Please run 'make librabbitmq' first.")
endif()

message(STATUS "RabbitMQ include dir: ${RABBITMQ_INCLUDE_DIR}")
message(STATUS "RabbitMQ library: ${RABBITMQ_LIBRARY}")

add_executable(sender_test sender_test.cpp ${PROTO_SRC} ${UTILS_SRCS})
target_link_libraries(sender_test PUBLIC ${RABBITMQ_LIBRARY} protobuf::libprotobuf pthread stdc++fs)
target_include_directories(sender_test PUBLIC ${RABBITMQ_INCLUDE_DIR})

add_executable(receiver_test receiver_test.cpp ${PROTO_SRC})
target_link_libraries(receiver_test PUBLIC ${RABBITMQ_LIBRARY} protobuf::libprotobuf pthread stdc++fs)
target_include_directories(receiver_test PUBLIC ${RABBITMQ_INCLUDE_DIR})

add_executable(sender_async_test sender_async_test.cpp ${PROTO_SRC} ${UTILS_SRCS})
target_link_libraries(sender_async_test PUBLIC ${RABBITMQ_LIBRARY} protobuf::libprotobuf Threads::Threads stdc++fs)
target_include_directories(sender_async_test PUBLIC ${RABBITMQ_INCLUDE_DIR})

add_executable(receiver_async_test receiver_async_test.cpp ${PROTO_SRC})
target_link_libraries(receiver_async_test PUBLIC ${RABBITMQ_LIBRARY} protobuf::libprotobuf Threads::Threads stdc++fs)
target_include_directories(receiver_async_test PUBLIC ${RABBITMQ_INCLUDE_DIR})

