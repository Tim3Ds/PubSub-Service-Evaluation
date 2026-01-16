cmake_minimum_required(VERSION 3.10)
project(nats_cpp_examples)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_STANDARD 11)
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

add_executable(sender_test sender_test.cpp)
target_link_libraries(sender_test PRIVATE nats_static)

add_executable(receiver_test receiver_test.cpp)
target_link_libraries(receiver_test PRIVATE nats_static)

