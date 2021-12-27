#pragma once

#include <cstdint>
#include <atomic>
#include <memory>
#include <vector>
#include <functional>
#include <shared_mutex>

namespace combotree {

class ALevel;
class BLevel;
class Manifest;
class PmemKV;

struct Pair {
  uint64_t key;
  uint64_t value;
};

class ComboTree {
 public:
  ComboTree(std::string pool_dir, size_t pool_size, bool create = true);
  ~ComboTree();

  bool Put(uint64_t key, uint64_t value);
  bool Update(uint64_t key, uint64_t value);
  bool Get(uint64_t key, uint64_t& value) const;
  bool Delete(uint64_t key);
  size_t Scan(uint64_t min_key, uint64_t max_key, size_t max_size,
      std::vector<std::pair<uint64_t, uint64_t>>& results);
  size_t Scan(uint64_t min_key, uint64_t max_key, size_t max_size,
      Pair* results);
  size_t Scan(uint64_t min_key, uint64_t max_key, size_t max_size,
      uint64_t* results);

  size_t Size() const;
  size_t CLevelCount() const;
  size_t BLevelEntries() const;
  void BLevelCompression() const;
  int64_t CLevelTime() const;
  uint64_t Usage() const;

  bool IsExpanding() const {
    return permit_delete_.load() == false;
  }

 private:
  class IterImpl;
  class NoSortIterImpl;

 public:
  class Iter {
   public:
    Iter(const ComboTree* tree);
    Iter(const ComboTree* tree, uint64_t start_key);
    ~Iter();

    uint64_t key() const;
    uint64_t value() const;
    bool next();
    bool end() const;

   private:
    IterImpl* pimpl_;
  };

  class NoSortIter {
   public:
    NoSortIter(const ComboTree* tree);
    NoSortIter(const ComboTree* tree, uint64_t start_key);
    ~NoSortIter();

    uint64_t key() const;
    uint64_t value() const;
    bool next();
    bool end() const;

   private:
    NoSortIterImpl* pimpl_;
  };

 private:
  const std::string POOL_LAYOUT = "Combo Tree";

  enum class State {
    USING_PMEMKV,
    PMEMKV_TO_COMBO_TREE,
    USING_COMBO_TREE,
    PREPARE_EXPANDING,
    COMBO_TREE_EXPANDING,
  };

  std::string pool_dir_;
  size_t pool_size_;
  ALevel* alevel_;
  BLevel* blevel_;
  BLevel* old_blevel_;
  PmemKV* pmemkv_;
  Manifest* manifest_;
  std::shared_mutex alevel_lock_;
  std::atomic<State> status_;
  std::atomic<bool> permit_delete_;
  std::atomic<int> sleeped_threads_;
  std::atomic<bool> need_sleep_;

  bool IsKeyInOldBLevel(uint64_t key, uint64_t& begin, uint64_t& end) const;
  bool ValidPoolDir_();
  void ChangeToComboTree_();
  void ExpandComboTree_();
  size_t Scan_(uint64_t min_key, uint64_t max_key, size_t max_size,
      size_t& count, void (*callback)(uint64_t,uint64_t,void*), void* arg,
      std::function<uint64_t()> cur_max_key);
};

} // namespace combotree