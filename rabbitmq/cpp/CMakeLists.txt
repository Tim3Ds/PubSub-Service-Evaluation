cmake_minimum_required(VERSION 3.10)
project(rabbitmq_examples)

set(CMAKE_CXX_STANDARD 11)
find_package(Threads REQUIRED)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Find rabbitmq-c using the local build prefix
find_path(RABBITMQ_INCLUDE_DIR amqp.h PATHS ${CMAKE_PREFIX_PATH}/include NO_DEFAULT_PATH)
find_library(RABBITMQ_LIBRARY NAMES rabbitmq PATHS ${CMAKE_PREFIX_PATH}/lib NO_DEFAULT_PATH)

if (NOT RABBITMQ_INCLUDE_DIR OR NOT RABBITMQ_LIBRARY)
    message(FATAL_ERROR "rabbitmq-c not found. Please run 'make librabbitmq' first.")
endif()

message(STATUS "RabbitMQ include dir: ${RABBITMQ_INCLUDE_DIR}")
message(STATUS "RabbitMQ library: ${RABBITMQ_LIBRARY}")

add_executable(sender_test sender_test.cpp)
target_link_libraries(sender_test PUBLIC ${RABBITMQ_LIBRARY} pthread)
target_include_directories(sender_test PUBLIC ${RABBITMQ_INCLUDE_DIR})

add_executable(receiver_test receiver_test.cpp)
target_link_libraries(receiver_test PUBLIC ${RABBITMQ_LIBRARY} pthread)
target_include_directories(receiver_test PUBLIC ${RABBITMQ_INCLUDE_DIR})

add_executable(sender_async_test sender_async_test.cpp)
target_link_libraries(sender_async_test PUBLIC ${RABBITMQ_LIBRARY} Threads::Threads)
target_include_directories(sender_async_test PUBLIC ${RABBITMQ_INCLUDE_DIR})

add_executable(receiver_async_test receiver_async_test.cpp)
target_link_libraries(receiver_async_test PUBLIC ${RABBITMQ_LIBRARY} Threads::Threads)
target_include_directories(receiver_async_test PUBLIC ${RABBITMQ_INCLUDE_DIR})

