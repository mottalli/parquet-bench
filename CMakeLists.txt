cmake_minimum_required(VERSION 2.8)
project(parquet-bench)

find_package(OpenMP REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS} -std=c++11 -Wall")

add_executable(parquet-bench main.cpp)
target_link_libraries(parquet-bench parquet)
