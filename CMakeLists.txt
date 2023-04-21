project(ComboTree)
cmake_minimum_required(VERSION 3.0)

# use `cmake -DSERVER:BOOL=ON ..` when running in server
option(USE_LIBPMEM      "libpmem or libvmmalloc"  ON)
option(BUF_SORT         "Sort buffer in KVBufer"  OFF)
option(STREAMING_LOAD   "Use Non-temporal Load"   OFF)
option(STREAMING_STORE  "Use Non-temporal Store"  OFF)
option(NO_LOCK          "Don't use lock"          OFF)
option(BRANGE           "Multi-thread expanding"  ON)

# ComboTree Configuration
# use `make clean && make CXX_DEFINES="-DNAME=VALUE"` to override during compile
if(BRANGE)
  set(EXPAND_THREADS      4)
endif(BRANGE)

set(CLEVEL_PMEM_FILE_SIZE "(1024*1024*1024*16UL)")

# how many kvs does blevel entry can store without creating its clevel tree
set(BLEVEL_EXPAND_BUF_KEY 6)

# the factor to trigger expansion, see ComboTree::Put()
set(EXPANSION_FACTOR      8)

# span factor
set(DEFAULT_SPAN          2)

# in the beginning, kv is stored in libpmemkv, at how many kvs do we make a transition to ComboTree
set(PMEMKV_THRESHOLD      3000)

# preallocate extra blevel entries when doing expansion, see BLevel::BLevel()
set(ENTRY_SIZE_FACTOR     1.2)

configure_file(
  "${PROJECT_SOURCE_DIR}/src/combotree_config.h.in"
  "${PROJECT_SOURCE_DIR}/src/combotree_config.h"
)

# -march=native used to detect CPU flags and use corrosponding flush instruction
set(CMAKE_CXX_FLAGS "-march=native -Wall")

# Require C++17
set(CMAKE_CXX_STANDARD_REQUIRED on)
set(CMAKE_CXX_STANDARD 17)

# Debug or Release
# Use command `cmake -DCMAKE_BUILD_TYPE=Release ..` to make a release build
if(CMAKE_BUILD_TYPE MATCHES Release)
  message(STATUS "Release build.")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2")
  add_definitions(-DNDEBUG)
else()
  message(STATUS "Debug build.")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
endif()

# combotree shared library
include_directories(include)

set(COMBO_TREE_SRC
      src/alevel.cc
      src/blevel.cc
      src/clevel.cc
      src/combotree.cc
      src/pmemkv.cc
)

add_library(combotree SHARED ${COMBO_TREE_SRC})
target_link_libraries(combotree pmem pmemkv pthread)

add_executable(combotree_example example/combotree_example.cc)
target_link_libraries(combotree_example combotree)
