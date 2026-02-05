cmake_minimum_required(VERSION 3.10)
project(nats_cpp_examples)

# Use centralized protobuf
set(CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH};/home/tim/repos/utils/protobuf/build")
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find protobuf package
find_package(Protobuf REQUIRED)

# Copy protobuf generated files from utils/cpp
set(MESSAGING_PROTO "${CMAKE_SOURCE_DIR}/../../utils/cpp/messaging.proto")
set(MESSAGING_PROTO_SRC "${CMAKE_SOURCE_DIR}/../../utils/cpp/messaging.pb.cc")
set(MESSAGING_PROTO_HDR "${CMAKE_SOURCE_DIR}/../../utils/cpp/messaging.pb.h")
set(MESSAGING_GRPC_SRC "${CMAKE_SOURCE_DIR}/../../utils/cpp/messaging.grpc.pb.cc")
set(MESSAGING_GRPC_HDR "${CMAKE_SOURCE_DIR}/../../utils/cpp/messaging.grpc.pb.h")
set(TEST_DATA_LOADER_SRC "${CMAKE_SOURCE_DIR}/../../utils/cpp/test_data_loader.cpp")
set(TEST_DATA_LOADER_HDR "${CMAKE_SOURCE_DIR}/../../utils/cpp/test_data_loader.hpp")
set(JSON_HDR "${CMAKE_SOURCE_DIR}/../../utils/cpp/json.hpp")

# Copy protobuf files to build directory if they exist
if(EXISTS "${MESSAGING_PROTO_SRC}")
    configure_file(${MESSAGING_PROTO_SRC} ${CMAKE_BINARY_DIR}/messaging.pb.cc COPYONLY)
    configure_file(${MESSAGING_PROTO_HDR} ${CMAKE_BINARY_DIR}/messaging.pb.h COPYONLY)
    configure_file(${MESSAGING_GRPC_SRC} ${CMAKE_BINARY_DIR}/messaging.grpc.pb.cc COPYONLY)
    configure_file(${MESSAGING_GRPC_HDR} ${CMAKE_BINARY_DIR}/messaging.grpc.pb.h COPYONLY)
    configure_file(${TEST_DATA_LOADER_SRC} ${CMAKE_BINARY_DIR}/test_data_loader.cpp COPYONLY)
    configure_file(${TEST_DATA_LOADER_HDR} ${CMAKE_BINARY_DIR}/test_data_loader.hpp COPYONLY)
    configure_file(${JSON_HDR} ${CMAKE_BINARY_DIR}/json.hpp COPYONLY)
    
    # Add protobuf source to a library
    add_library(messaging_lib
        ${CMAKE_BINARY_DIR}/messaging.pb.cc
        ${CMAKE_BINARY_DIR}/messaging.grpc.pb.cc
        ${CMAKE_BINARY_DIR}/test_data_loader.cpp
    )
    target_link_libraries(messaging_lib PUBLIC protobuf::libprotobuf stdc++fs)
endif()

find_package(Threads REQUIRED)
# Download and build nats.c if not found
include(FetchContent)
FetchContent_Declare(
    natsc
    GIT_REPOSITORY https://github.com/nats-io/nats.c.git
    GIT_TAG        main
)

FetchContent_MakeAvailable(natsc)

# Copy nats.h and related headers to ${CMAKE_BINARY_DIR}/include
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/include/nats)
file(GLOB NATS_HEADERS "${natsc_SOURCE_DIR}/src/*.h")
foreach(hdr ${NATS_HEADERS})
    configure_file(${hdr} ${CMAKE_BINARY_DIR}/include/nats/ COPYONLY)
endforeach()


# nats.c builds a static library in natsc-build/src/libnats.a
# The include directory is now ${CMAKE_BINARY_DIR}/include/nats
include_directories(BEFORE SYSTEM ${CMAKE_BINARY_DIR}/include)
include_directories(BEFORE SYSTEM ${CMAKE_BINARY_DIR}/include/nats)

# Add protobuf include directory
if(EXISTS "${MESSAGING_PROTO_SRC}")
    include_directories(BEFORE SYSTEM ${CMAKE_BINARY_DIR})
    add_executable(sender_test sender_test.cpp)
    target_link_libraries(sender_test PRIVATE nats_static messaging_lib stdc++fs)
    
    add_executable(receiver_test receiver_test.cpp)
    target_link_libraries(receiver_test PRIVATE nats_static messaging_lib stdc++fs)
    
    add_executable(sender_async_test sender_async_test.cpp)
    target_link_libraries(sender_async_test PRIVATE nats_static messaging_lib Threads::Threads stdc++fs)
    
    add_executable(receiver_async_test receiver_async_test.cpp)
    target_link_libraries(receiver_async_test PRIVATE nats_static messaging_lib Threads::Threads stdc++fs)
else()
    # Fallback without protobuf
    add_executable(sender_test sender_test.cpp)
    target_link_libraries(sender_test PRIVATE nats_static stdc++fs)
    
    add_executable(receiver_test receiver_test.cpp)
    target_link_libraries(receiver_test PRIVATE nats_static stdc++fs)
    
    add_executable(sender_async_test sender_async_test.cpp)
    target_link_libraries(sender_async_test PRIVATE nats_static Threads::Threads stdc++fs)
    
    add_executable(receiver_async_test receiver_async_test.cpp)
    target_link_libraries(receiver_async_test PRIVATE nats_static Threads::Threads stdc++fs)
endif()

