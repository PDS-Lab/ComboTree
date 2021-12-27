#include <filesystem>
#include <libpmem.h>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include "combotree_config.h"

namespace combotree {

namespace {

const std::string DEFAULT_PMEMKV_PATH = "pmemkv";
const std::string DEFAULT_PMEM_PATH = "blevel";
const std::string DEFAULT_PMEMOBJ_PATH = "clevel";

} // anonymous namespace

class Manifest {
 public:
  Manifest(std::string dir, size_t size = PMEMOBJ_MIN_POOL, bool create = true)
      : dir_(dir), size_(size)
  {
    if (create) {
      std::string manifest_path = dir_ + "Manifest";
      std::filesystem::remove(manifest_path);
      pop_ = pmem::obj::pool<Root>::create(manifest_path,
          "Combo Tree Manifest", size_, 0666);
      root_ = pop_.root();
      pmem::obj::make_persistent_atomic<std::string>(
          pop_, root_->pmemkv_path, dir_ + DEFAULT_PMEMKV_PATH);
      pmem::obj::make_persistent_atomic<std::string>(
          pop_, root_->blevel_path, dir_ + DEFAULT_PMEM_PATH);
      pmem::obj::make_persistent_atomic<std::string>(
          pop_, root_->clevel_path, dir_ + DEFAULT_PMEMOBJ_PATH);
      root_->is_combo_tree = 0;
      root_->combo_tree_seq = 0;
      root_.persist();
    } else {
      assert(0);
    }
  }

  const std::string PmemKVPath() const {
    return *root_->pmemkv_path;
  }

  const std::string BLevelPath() const {
    return *root_->blevel_path;
  }

  const std::string CLevelPath() const {
    return *root_->clevel_path + std::string("-") +
           std::to_string(root_->combo_tree_seq);
  }

  void NewComboTreePath(size_t clevel_size) {
    std::filesystem::remove(CLevelPath());
    root_->combo_tree_seq++;
    std::filesystem::remove(CLevelPath());
  }

  bool IsComboTree() const {
    return root_->is_combo_tree;
  }

  void SetIsComboTree(int is_combo_tree) {
    pop_.memcpy_persist(&root_->is_combo_tree, &is_combo_tree,
        sizeof(is_combo_tree));
  }

 private:
  std::string dir_; // combotree directory
  size_t size_;     // manifest file size

  struct Root {
    pmem::obj::persistent_ptr<std::string> pmemkv_path;
    pmem::obj::persistent_ptr<std::string> blevel_path;
    pmem::obj::persistent_ptr<std::string> clevel_path;
    int combo_tree_seq;
    int is_combo_tree;
  };

  pmem::obj::pool<Root> pop_;
  pmem::obj::persistent_ptr<Root> root_;

  void* blevel_addr_;
  void* blevel_pmem_addr_;
  size_t blevel_mapped_len_;

};

} // namespace combotree