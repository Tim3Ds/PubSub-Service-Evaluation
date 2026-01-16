cmake_minimum_required(VERSION 3.10)
project(zeromq_examples)

find_package(PkgConfig REQUIRED)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Add local build/lib/pkgconfig to PKG_CONFIG_PATH for local ActiveMQ-CPP
set(ENV{PKG_CONFIG_PATH} "${CMAKE_BINARY_DIR}/lib/pkgconfig:${CMAKE_BINARY_DIR}/lib64/pkgconfig:$ENV{PKG_CONFIG_PATH}")
pkg_check_modules(ZMQ REQUIRED libzmq)
link_directories(${ZMQ_LIBRARY_DIRS}64)

message(STATUS "ZeroMQ include dirs: ${ZMQ_INCLUDE_DIRS}")
message(STATUS "ZeroMQ libraries: ${ZMQ_LIBRARIES}")
add_executable(sender_test sender_test.cpp)
target_link_libraries(sender_test PUBLIC ${ZMQ_LIBRARIES})
target_include_directories(sender_test PUBLIC ${ZMQ_INCLUDE_DIRS})

add_executable(receiver_test receiver_test.cpp)
target_link_libraries(receiver_test PUBLIC ${ZMQ_LIBRARIES})
target_include_directories(receiver_test PUBLIC ${ZMQ_INCLUDE_DIRS})