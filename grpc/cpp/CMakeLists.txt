cmake_minimum_required(VERSION 3.15)
project(grpc_pubsub)

set(CMAKE_CXX_STANDARD 17)

# Set default output directory for executables
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

# Proto file
set(PROTO_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../proto")
set(PROTO_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}")
set(PROTO_FILE "${PROTO_SRC_DIR}/pubsub.proto")

# Generated sources
set(PROTO_SRCS "${PROTO_BINARY_DIR}/pubsub.pb.cc")
set(PROTO_HDRS "${PROTO_BINARY_DIR}/pubsub.pb.h")
set(GRPC_SRCS "${PROTO_BINARY_DIR}/pubsub.grpc.pb.cc")
set(GRPC_HDRS "${PROTO_BINARY_DIR}/pubsub.grpc.pb.h")
# Force usage of local executables
set(_PROTOBUF_PROTOC "${CMAKE_PREFIX_PATH}/bin/protoc")
set(_GRPC_CPP_PLUGIN_EXECUTABLE "${CMAKE_PREFIX_PATH}/bin/grpc_cpp_plugin")

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

# TestData proto
set(TEST_PROTO_FILE "${PROTO_SRC_DIR}/test_data.proto")
set(TEST_PROTO_SRCS "${PROTO_BINARY_DIR}/test_data.pb.cc")
set(TEST_PROTO_HDRS "${PROTO_BINARY_DIR}/test_data.pb.h")
set(TEST_GRPC_SRCS "${PROTO_BINARY_DIR}/test_data.grpc.pb.cc")
set(TEST_GRPC_HDRS "${PROTO_BINARY_DIR}/test_data.grpc.pb.h")

add_custom_command(
  OUTPUT "${TEST_PROTO_SRCS}" "${TEST_PROTO_HDRS}" "${TEST_GRPC_SRCS}" "${TEST_GRPC_HDRS}"
  COMMAND ${_PROTOBUF_PROTOC}
  ARGS --grpc_out="${PROTO_BINARY_DIR}"
       --cpp_out="${PROTO_BINARY_DIR}"
       -I "${PROTO_SRC_DIR}"
       --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
       "${TEST_PROTO_FILE}"
  DEPENDS "${TEST_PROTO_FILE}"
)

add_library(test_data_proto
  ${TEST_PROTO_SRCS}
  ${TEST_GRPC_SRCS}
  ${TEST_PROTO_HDRS}
  ${TEST_GRPC_HDRS}
)
target_link_libraries(test_data_proto
  gRPC::grpc++
  protobuf::libprotobuf
)
target_include_directories(test_data_proto PUBLIC "${PROTO_BINARY_DIR}" "${Protobuf_INCLUDE_DIRS}")

# Targets
add_executable(sender_test sender_test.cpp)
target_link_libraries(sender_test test_data_proto gRPC::grpc++ protobuf::libprotobuf)

add_executable(receiver_test receiver_test.cpp)
target_link_libraries(receiver_test test_data_proto gRPC::grpc++ protobuf::libprotobuf)

