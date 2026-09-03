cmake_minimum_required(VERSION 3.18)
project(finsignal_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(FetchContent)
FetchContent_Declare(
  pybind11
  GIT_REPOSITORY https://github.com/pybind/pybind11.git
  GIT_TAG v2.13.6
)
FetchContent_MakeAvailable(pybind11)

add_library(finsignal_core STATIC
  src/event_engine.cpp
)
set_target_properties(finsignal_core PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(finsignal_core PUBLIC include)

pybind11_add_module(finsignal_engine src/bindings.cpp)
target_link_libraries(finsignal_engine PRIVATE finsignal_core)
target_include_directories(finsignal_engine PRIVATE include)
