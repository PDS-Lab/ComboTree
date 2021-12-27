# ComboTree

## 1 Introduction
This is the implementation of the paper "ComboTree: A Persistent Indexing Structure with Universal Operational Efficiency and Scalability" appeared in TPDS 2022. Papers available at XX(update later). We implement ComboTree and evaluate it on Intel’s Optane DC Persistent Memory module (DCPMM).

## 2 Compliation and Run

### 2.1 Tools
ComboTree acesses NVM via PMDK. To run ComboTree, please install [PMDK](https://github.com/pmem/pmdk), libpmemobj-cpp(https://github.com/pmem/libpmemobj-cpp) and pmemkv(https://github.com/pmem/pmemkv) first.

### 2.2 Compilation
```
> mkdir build
> cd build
> cmake ..
> make
```

### 2.3 Example
```
> cd build
# use parameter to set thread num , default thread number is 1
> ./combotree_example 4
```

## 3 Acknowledgement
We appreciate PingCap and Intel for the hardware support and maintenance.

## 4 Contributors
- Ph.D supervisor: Jiguang Wan (jgwan@hust.edu.cn)
- Zhonghua Wang (wangzhonghua@hust.edu.cn)
- Ting Yao (tingyao@hust.edu.cn)
- Qiuyang Zhang (qyzhang@hust.edu.cn)
- Junyue Wang (junyuewang@hust.edu.cn)
- Yiwen Zhang (zhangyiwen@hust.edu.cn)