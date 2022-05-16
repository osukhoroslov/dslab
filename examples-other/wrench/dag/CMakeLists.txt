cmake_minimum_required(VERSION 3.2)
message(STATUS "Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")

set(CMAKE_CXX_STANDARD 17)

project(wrench-example-real-workflow)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

find_package(Boost REQUIRED)
find_package(SimGrid 3.31 REQUIRED)

find_library(WRENCH_LIBRARY NAMES wrench)
find_library(WRENCH_WFCOMMONS_WORKFLOW_PARSER_LIBRARY NAMES wrenchwfcommonsworkflowparser)

include_directories(${SimGrid_INCLUDE_DIR})
include_directories(/usr/local/include)
include_directories(/usr/local/include/wrench)

message(STATUS "SimGrid_PATH: ${SimGrid_PATH}")
message(STATUS "SimGrid_INCLUDE_DIR: ${SimGrid_INCLUDE_DIR}")
message(STATUS "SimGrid_LIBRARY: ${SimGrid_LIBRARY}")

set(SOURCE_FILES
    SimpleWMS.h
    SimpleWMS.cpp
    SimpleWorkflowSimulator.cpp
)

add_executable(wrench-example-real-workflow ${SOURCE_FILES})

if (ENABLE_BATSCHED)
    target_link_libraries(wrench-example-real-workflow
            ${WRENCH_LIBRARY}
            ${WRENCH_WFCOMMONS_WORKFLOW_PARSER_LIBRARY}
            ${SimGrid_LIBRARY}
            ${Boost_LIBRARIES}
            -lzmq )
else()
    target_link_libraries(wrench-example-real-workflow
            ${WRENCH_LIBRARY}
            ${WRENCH_WFCOMMONS_WORKFLOW_PARSER_LIBRARY}
            ${SimGrid_LIBRARY}
            ${Boost_LIBRARIES}
            )
endif()
