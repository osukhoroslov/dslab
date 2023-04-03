add_executable(network
    network.cpp
)

target_link_libraries(network simgrid)
target_include_directories(network PUBLIC SYSTEM ${SimGrid_INCLUDE_DIR})
