//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  auto bucket = std::make_shared<Bucket>(bucket_size);
  dir_.emplace_back(bucket);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  auto my_list = bucket->GetItems();
  for (const auto &pa : my_list) {
    bucket->Remove(pa.first);
    size_t dir_index = IndexOf(pa.first);
    dir_[dir_index]->Insert(pa.first, pa.second);
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_index = IndexOf(key);
  return dir_[dir_index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_index = IndexOf(key);
  return dir_[dir_index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  // find key
  size_t dir_index = IndexOf(key);
  auto bucket_ptr = dir_[dir_index];
  if (bucket_ptr->Insert(key, value)) {
    return;
  }
  while (bucket_ptr->IsFull()) {
    if (bucket_ptr->GetDepth() == global_depth_) {
      global_depth_++;
      std::vector<std::shared_ptr<Bucket>> dir_copy(dir_);
      dir_.insert(dir_.end(), dir_copy.begin(), dir_copy.end());
    }
    int pre_local_depth = bucket_ptr->GetDepth();
    bucket_ptr->IncrementDepth();
    auto bucket2_ptr = std::make_shared<Bucket>(bucket_size_, bucket_ptr->GetDepth());
    num_buckets_++;
    // resort bucket_ptr
    int pre_mask = (1 << pre_local_depth) - 1;
    int bro_low_bit = dir_index & pre_mask;
    int size = dir_.size();
    for (int i = 0; i < size; i++) {
      if ((i & pre_mask) == bro_low_bit) {
        if (((i >> pre_local_depth) & 1) == 1) {
          dir_[i] = bucket2_ptr;
        }
      }
    }
    // resort {k,v} in old bucket_ptr
    RedistributeBucket(bucket_ptr);
    // recalculate dir_index
    dir_index = IndexOf(key);
    bucket_ptr = dir_[dir_index];
    if (bucket_ptr->Insert(key, value)) {
      break;
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // traverse the bucket's list
  for (auto &pair : list_) {
    if (pair.first == key) {
      value = pair.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto iter = list_.begin(); iter != list_.end(); ++iter) {
    if (iter->first == key) {
      list_.erase(iter);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto &pair : list_) {
    if (pair.first == key) {
      pair.second = value;
      return true;
    }
  }
  if (IsFull()) {
    return false;
  }
  // insert this node
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
