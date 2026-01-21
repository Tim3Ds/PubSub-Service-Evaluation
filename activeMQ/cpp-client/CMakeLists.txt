cmake_minimum_required(VERSION 3.5)
project(activemq_cpp_examples)

find_package(PkgConfig REQUIRED)

# Add local build/lib/pkgconfig to PKG_CONFIG_PATH for local ActiveMQ-CPP
set(ENV{PKG_CONFIG_PATH} "${CMAKE_SOURCE_DIR}/build/lib/pkgconfig:$ENV{PKG_CONFIG_PATH}")

# Set default output directory for executables
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Try to find activemq-cpp via pkg-config or default locations
pkg_check_modules(ACTIVEMQCPP activemq-cpp)

if(ACTIVEMQCPP_FOUND)
  include_directories(${ACTIVEMQCPP_INCLUDE_DIRS})
  link_directories(${ACTIVEMQCPP_LIBRARY_DIRS} ${CMAKE_SOURCE_DIR}/lib_fix)
else()
  # Allow user to point to installed headers/libs via environment
  include_directories(/usr/local/include)
  link_directories(/usr/local/lib ${CMAKE_SOURCE_DIR}/lib_fix)
endif()

add_executable(sender_test sender_test.cpp)
target_link_libraries(sender_test ${ACTIVEMQCPP_LIBRARIES} uuid stdc++ pthread)

add_executable(receiver_test receiver_test.cpp)
target_link_libraries(receiver_test ${ACTIVEMQCPP_LIBRARIES} uuid stdc++ pthread)


