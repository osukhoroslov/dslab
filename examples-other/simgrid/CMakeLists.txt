cmake_minimum_required(VERSION 3.1)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror")

project(SimGridExamples)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

find_package(SimGrid 3.31 REQUIRED)

message(STATUS "SimGrid_PATH: ${SimGrid_PATH}")
message(STATUS "SimGrid_INCLUDE_DIR: ${SimGrid_INCLUDE_DIR}")
message(STATUS "SimGrid_LIBRARY: ${SimGrid_LIBRARY}")

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_subdirectory(master-workers)
add_subdirectory(ping-pong)
add_subdirectory(storage)
