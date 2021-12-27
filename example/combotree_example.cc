#undef NDEBUG

#include <iostream>
#include <cassert>
#include <iomanip>
#include <thread>
#include <map>
#include "combotree/combotree.h"
#include "combotree_config.h"
#include "random.h"

#define TEST_SIZE   4000000

using combotree::ComboTree;
using combotree::Random;

int thread_num = 1;

int main(int argc,char* argv[]) {
  if(argc > 2){
    std::cout << "wrong param numbers!" <<std::endl;
    return 0;
  }else if(argc == 2){
    thread_num = atoi(argv[1]);
  }
#ifdef SERVER
  ComboTree* tree = new ComboTree("/mnt/pmem/combotree/", (1024*1024*1024*100UL), true);
#else
  ComboTree* tree = new ComboTree("/mnt/pmem0/", (1024*1024*512UL), true);
#endif
  std::cout << "THREAD NUM:            " << thread_num <<std::endl;
  std::cout << "TEST SIZE:             " << TEST_SIZE << std::endl;

  std::vector<uint64_t> key;
  Random rnd(0, TEST_SIZE-1);
  for (int i = 0; i < TEST_SIZE; ++i)
    key.push_back(i);
  for (int i = 0; i < TEST_SIZE; ++i)
    std::swap(key[i],key[rnd.Next()]);

  std::cout << "finish generate data" << std::endl;

  std::vector<std::thread> threads;
  size_t per_thread_size = TEST_SIZE / thread_num;

  // PUT
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back([=,&key](){
      uint64_t start_pos = i*per_thread_size;
      for (size_t j = 0; j < per_thread_size; ++j)
        assert(tree->Put(key[start_pos+j], key[start_pos+j]) == true);
    });
  }
  for (auto& t : threads)
    t.join();
  threads.clear();

  // UPDATE
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back([=,&key](){
      uint64_t start_pos = i*per_thread_size;
      for (size_t j = 0; j < per_thread_size; ++j)
        assert(tree->Update(key[start_pos+j], key[start_pos+j]+1) == true);
    });
  }
  for (auto& t : threads)
    t.join();
  threads.clear();

  // Multi GET
  for (int i = 0; i < thread_num; ++i) {
    threads.emplace_back([=,&key](){
      uint64_t start_pos = i*per_thread_size;
      uint64_t value;
      for (size_t j = 0; j < per_thread_size; ++j) {
        assert(tree->Get(key[start_pos+j], value) == true);
        assert(value == key[start_pos+j]+1);
      }
    });
  }
  for (auto& t : threads)
    t.join();
  threads.clear();

  // Single GET
  for (auto& k : key) {
    uint64_t value;
    assert(tree->Get(k, value) == true);
    assert(value == k+1);
  }

  // Single GET
  for (uint64_t i = TEST_SIZE; i < TEST_SIZE + 100000; ++i) {
    uint64_t value;
    assert(tree->Get(i, value) == false);
  }

  return 0;
}