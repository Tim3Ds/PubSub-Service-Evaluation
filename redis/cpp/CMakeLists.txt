project(redis_tests)
cmake_minimum_required(VERSION 3.10)

set(CMAKE_CXX_STANDARD 11)
find_package(Threads REQUIRED)

# Find Hiredis
find_path(HIREDIS_INCLUDE_DIR hiredis/hiredis.h PATHS ${CMAKE_PREFIX_PATH}/include)
find_library(HIREDIS_LIBRARY hiredis PATHS ${CMAKE_PREFIX_PATH}/lib)

include_directories(${HIREDIS_INCLUDE_DIR})
include_directories(/home/tim/repos/include)

add_executable(sender_test sender_test.cpp)
target_link_libraries(sender_test ${HIREDIS_LIBRARY})

add_executable(receiver_test receiver_test.cpp)
target_link_libraries(receiver_test ${HIREDIS_LIBRARY})

add_executable(sender_async_test sender_async_test.cpp)
target_link_libraries(sender_async_test ${HIREDIS_LIBRARY} Threads::Threads)

add_executable(receiver_async_test receiver_async_test.cpp)
target_link_libraries(receiver_async_test ${HIREDIS_LIBRARY} Threads::Threads)
