#include <random>

namespace combotree {

class Random {
 public:
  Random(uint64_t start, uint64_t end)
    : rng(std::random_device()()),
      dist(start, end)
  {}

  uint64_t Next() {
    return dist(rng);
  }

 private:
  std::mt19937 rng;
  std::uniform_int_distribution<std::mt19937::result_type> dist;

};

}