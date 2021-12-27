#pragma once

#include <cassert>
#include <cstdlib>
#include <libpmemkv.hpp>
#include <vector>
#include <atomic>
#include <mutex>
#include <algorithm>
#include <vector>

namespace combotree {

using pmem::kv::status;
using pmem::kv::string_view;

namespace {

const uint64_t SIZE = 512 * 1024UL * 1024UL;

} // anonymous namespace

class PmemKV {
 public:
  explicit PmemKV(std::string path, size_t size = SIZE,
                  std::string engine = "cmap", bool force_create = true);

  bool Put(uint64_t key, uint64_t value);
  bool Update(uint64_t key, uint64_t value);
  bool Get(uint64_t key, uint64_t& value) const;
  bool Delete(uint64_t key);
  size_t Scan(uint64_t min_key, uint64_t max_key, uint64_t max_size,
              void (*callback)(uint64_t,uint64_t,void*), void* arg) const;
  size_t Scan(uint64_t min_key, uint64_t max_key, uint64_t max_size,
              std::vector<std::pair<uint64_t,uint64_t>>& kv) const;

  size_t Size() const {
    ReadRef_();
    if (!read_valid_.load(std::memory_order_acquire))
      return -1;
    size_t size;
    [[maybe_unused]] auto s = db_->count_all(size);
    assert(s == status::OK);
    ReadUnRef_();
    return size;
  }

  bool NoWriteRef() const { return write_ref_.load() == 0; }
  bool NoReadRef() const { return read_ref_.load() == 0; }

  static void SetWriteValid() {
    write_valid_.store(true, std::memory_order_release);
  }

  static void SetWriteUnvalid() {
    write_valid_.store(false, std::memory_order_release);
  }

  static void SetReadValid() {
    read_valid_.store(true, std::memory_order_release);
  }

  static void SetReadUnvalid() {
    read_valid_.store(false, std::memory_order_release);
  }

 private:
  pmem::kv::db* db_;
  mutable std::atomic<int> write_ref_;
  mutable std::atomic<int> read_ref_;

  static std::atomic<bool> write_valid_;
  static std::atomic<bool> read_valid_;

  void WriteRef_() const { write_ref_++; }
  void WriteUnRef_() const { write_ref_--; }
  void ReadRef_() const { read_ref_++; }
  void ReadUnRef_() const { read_ref_--; }
};

} // namespace combotree