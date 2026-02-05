cmake_minimum_required(VERSION 3.5)
project(activemq_cpp_examples)

# Use centralized protobuf
set(CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH};/home/tim/repos/utils/protobuf/build")
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

find_package(PkgConfig REQUIRED)

# Add local build/lib/pkgconfig to PKG_CONFIG_PATH for local ActiveMQ-CPP
set(ENV{PKG_CONFIG_PATH} "${CMAKE_SOURCE_DIR}/build/lib/pkgconfig:$ENV{PKG_CONFIG_PATH}")

# Get repo root using git
execute_process(
    COMMAND git rev-parse --show-toplevel
    OUTPUT_VARIABLE REPO_ROOT
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

set(CMAKE_CXX_STANDARD 17)

# Set default output directory for executables
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Try to find activemq-cpp via pkg-config or default locations
pkg_check_modules(ACTIVEMQCPP activemq-cpp)

include_directories(${REPO_ROOT}/utils/cpp)
include_directories(${REPO_ROOT}/utils)

if(ACTIVEMQCPP_FOUND)
  include_directories(${ACTIVEMQCPP_INCLUDE_DIRS})
  link_directories(${ACTIVEMQCPP_LIBRARY_DIRS} ${CMAKE_SOURCE_DIR}/lib_fix)
else()
  # Allow user to point to installed headers/libs via environment
  include_directories(/usr/local/include)
  link_directories(/usr/local/lib ${CMAKE_SOURCE_DIR}/lib_fix)
endif()

set(UTILS_SRCS "${REPO_ROOT}/utils/cpp/test_data_loader.cpp" "${REPO_ROOT}/utils/cpp/messaging.pb.cc")

add_executable(sender_test sender_test.cpp ${UTILS_SRCS})
target_link_libraries(sender_test ${ACTIVEMQCPP_LIBRARIES} uuid stdc++ pthread protobuf::libprotobuf stdc++fs)

add_executable(receiver_test receiver_test.cpp ${UTILS_SRCS})
target_link_libraries(receiver_test ${ACTIVEMQCPP_LIBRARIES} uuid stdc++ pthread protobuf::libprotobuf stdc++fs)

add_executable(sender_async_test sender_async_test.cpp ${UTILS_SRCS})
target_link_libraries(sender_async_test ${ACTIVEMQCPP_LIBRARIES} uuid stdc++ pthread protobuf::libprotobuf stdc++fs)

add_executable(receiver_async_test receiver_async_test.cpp ${UTILS_SRCS})
target_link_libraries(receiver_async_test ${ACTIVEMQCPP_LIBRARIES} uuid stdc++ pthread protobuf::libprotobuf stdc++fs)


