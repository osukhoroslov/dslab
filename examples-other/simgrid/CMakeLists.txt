cmake_minimum_required(VERSION 3.11)

project(SimGridExamples)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")
find_package(SimGrid 3.35 REQUIRED)
message(STATUS "SimGrid_PATH: ${SimGrid_PATH}")
message(STATUS "SimGrid_INCLUDE_DIR: ${SimGrid_INCLUDE_DIR}")
message(STATUS "SimGrid_LIBRARY: ${SimGrid_LIBRARY}")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

include(FetchContent)
FetchContent_Declare(
  argparse
  GIT_REPOSITORY https://github.com/p-ranav/argparse.git
  GIT_TAG        40a3888f151256bba95c45860af381b2b73752e8
)
FetchContent_MakeAvailable(argparse)

add_subdirectory(master-workers)
add_subdirectory(network)
add_subdirectory(ping-pong)
add_subdirectory(storage)
