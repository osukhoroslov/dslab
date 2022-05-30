add_executable(ping-pong ping-pong.cpp)

target_link_libraries(ping-pong ${SimGrid_LIBRARY})
target_include_directories(ping-pong PUBLIC SYSTEM ${SimGrid_INCLUDE_DIR})
