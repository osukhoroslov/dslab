add_executable(network
    network.cpp
)

target_link_libraries(network ${SimGrid_LIBRARY})
target_include_directories(network PUBLIC SYSTEM ${SimGrid_INCLUDE_DIR})
