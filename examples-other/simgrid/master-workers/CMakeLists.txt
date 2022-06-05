add_executable(master-workers 
    master-workers.cpp
    master.cpp
    worker.cpp
    client.cpp
)

target_link_libraries(master-workers ${SimGrid_LIBRARY})
target_include_directories(master-workers PUBLIC SYSTEM ${SimGrid_INCLUDE_DIR})
