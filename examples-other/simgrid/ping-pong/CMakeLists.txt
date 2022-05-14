cmake_minimum_required(VERSION 3.1)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "-O2")

project(PingPong)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

find_package(SimGrid 3.31 REQUIRED)

include_directories(${SimGrid_INCLUDE_DIR})

message(STATUS "SimGrid_PATH: ${SimGrid_PATH}")
message(STATUS "SimGrid_INCLUDE_DIR: ${SimGrid_INCLUDE_DIR}")
message(STATUS "SimGrid_LIBRARY: ${SimGrid_LIBRARY}")

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(ping-pong ping-pong.cpp)
target_link_libraries(ping-pong ${SimGrid_LIBRARY})
