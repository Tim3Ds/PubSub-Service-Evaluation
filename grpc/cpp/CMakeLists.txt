cmake_minimum_required(VERSION 3.15)
project(grpc_pubsub)

set(CMAKE_CXX_STANDARD 17)
find_package(Threads REQUIRED)

# Set default output directory for executables
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Get repo root using git
execute_process(
    COMMAND git rev-parse --show-toplevel
    OUTPUT_VARIABLE REPO_ROOT
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Set path to local deps using repo root
set(LOCAL_DEPS "${REPO_ROOT}/grpc/cpp/build/deps")
set(CMAKE_PREFIX_PATH ${LOCAL_DEPS} ${CMAKE_PREFIX_PATH})

find_package(Protobuf REQUIRED HINTS ${LOCAL_DEPS})
find_package(gRPC REQUIRED HINTS ${LOCAL_DEPS})

include_directories(${LOCAL_DEPS}/include)
include_directories(${REPO_ROOT}/utils)
include_directories(${REPO_ROOT}/utils/cpp)

# Proto file (using top-level messaging.proto)
set(PROTO_SRC_DIR "${REPO_ROOT}/utils")
set(PROTO_BINARY_DIR "${REPO_ROOT}/utils/cpp")
set(PROTO_FILE "${PROTO_SRC_DIR}/messaging.proto")

# Generated sources
set(PROTO_SRCS "${PROTO_BINARY_DIR}/messaging.pb.cc")
set(PROTO_HDRS "${PROTO_BINARY_DIR}/messaging.pb.h")
set(GRPC_SRCS "${PROTO_BINARY_DIR}/messaging.grpc.pb.cc")
set(GRPC_HDRS "${PROTO_BINARY_DIR}/messaging.grpc.pb.h")
# Force correct programs
set(_PROTOBUF_PROTOC "${CMAKE_CURRENT_SOURCE_DIR}/build/deps/bin/protoc")
set(_GRPC_CPP_PLUGIN_EXECUTABLE "${CMAKE_CURRENT_SOURCE_DIR}/build/deps/bin/grpc_cpp_plugin")

# Rule to generate C++ code from .proto
add_custom_command(
  OUTPUT "${PROTO_SRCS}" "${PROTO_HDRS}" "${GRPC_SRCS}" "${GRPC_HDRS}"
  COMMAND ${_PROTOBUF_PROTOC}
  ARGS --grpc_out="${PROTO_BINARY_DIR}"
       --cpp_out="${PROTO_BINARY_DIR}"
       -I "${PROTO_SRC_DIR}"
       --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
       "${PROTO_FILE}"
  DEPENDS "${PROTO_FILE}"
)

add_library(messaging_proto
  ${PROTO_SRCS}
  ${GRPC_SRCS}
  ${PROTO_HDRS}
  ${GRPC_HDRS}
  "${REPO_ROOT}/utils/cpp/test_data_loader.cpp"
)
target_link_libraries(messaging_proto
  gRPC::grpc++
  protobuf::libprotobuf
)
target_include_directories(messaging_proto PUBLIC "${PROTO_BINARY_DIR}" "${Protobuf_INCLUDE_DIRS}")

# Targets
add_executable(sender_test sender_test.cpp)
target_link_libraries(sender_test messaging_proto gRPC::grpc++ protobuf::libprotobuf stdc++fs)

add_executable(receiver_test receiver_test.cpp)
target_link_libraries(receiver_test messaging_proto gRPC::grpc++ protobuf::libprotobuf stdc++fs)

add_executable(sender_async_test sender_async_test.cpp)
target_link_libraries(sender_async_test messaging_proto gRPC::grpc++ protobuf::libprotobuf Threads::Threads stdc++fs)

add_executable(receiver_async_test receiver_async_test.cpp)
target_link_libraries(receiver_async_test messaging_proto gRPC::grpc++ protobuf::libprotobuf Threads::Threads stdc++fs)

