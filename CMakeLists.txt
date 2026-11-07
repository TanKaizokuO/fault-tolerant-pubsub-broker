cmake_minimum_required(VERSION 3.16)

project(fault_tolerant_pubsub_broker LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(SANITIZE "Build with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

find_package(Threads REQUIRED)

# Shared settings for every C++ target: vendored headers, threading, warnings,
# and optional sanitizers.
add_library(pubsub_common INTERFACE)
target_include_directories(pubsub_common INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
target_link_libraries(pubsub_common INTERFACE Threads::Threads)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(pubsub_common INTERFACE -Wall -Wextra)
endif()

if(SANITIZE)
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    message(FATAL_ERROR "SANITIZE=ON requires a GCC or Clang compiler")
  endif()
  target_compile_options(pubsub_common INTERFACE
    -fsanitize=address,undefined
    -fno-omit-frame-pointer
    -fno-sanitize-recover=undefined)
  target_link_options(pubsub_common INTERFACE
    -fsanitize=address,undefined)
endif()

enable_testing()

# Broker: REST endpoints, registry, and delivery with timeouts and retries.
add_executable(broker
  broker/main.cpp
  broker/registry.cpp
  broker/delivery.cpp)
target_link_libraries(broker PRIVATE pubsub_common)

# C++ subscriber: same REST contract as subscribers/subscriber.py.
add_executable(subscriber
  subscribers/subscriber.cpp)
target_link_libraries(subscriber PRIVATE pubsub_common)

# Registry unit tests (plain asserts), run through ctest.
add_executable(registry_test
  tests/registry_test.cpp
  broker/registry.cpp)
target_link_libraries(registry_test PRIVATE pubsub_common)

add_test(NAME registry_test COMMAND registry_test)
