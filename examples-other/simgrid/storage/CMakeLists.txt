add_executable(storage
    main.cpp
    disk.cpp
)

target_link_libraries(storage ${SimGrid_LIBRARY} argparse::argparse)
target_include_directories(storage PUBLIC SYSTEM ${SimGrid_INCLUDE_DIR})
