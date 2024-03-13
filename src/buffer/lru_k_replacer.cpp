//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  if (curr_evictable_size_ == 0) {
    latch_.unlock();
    return false;
  }
  int evict_access_in_fifo = INT32_MAX;
  int evict_access_in_lru = INT32_MAX;
  int evict_id_in_fifo = 0;
  int evict_id_in_lru = 0;
  for (auto &pair : lru_map_) {
    if (!pair.second.evictable_) {
      continue;
    }
    if (pair.second.access_count_ < static_cast<int>(k_) && pair.second.first_access_time_ < evict_access_in_fifo) {
      evict_access_in_fifo = pair.second.first_access_time_;
      evict_id_in_fifo = pair.first;
    }
    if (pair.second.access_count_ >= static_cast<int>(k_) && pair.second.last_access_time_ < evict_access_in_lru) {
      evict_access_in_lru = pair.second.last_access_time_;
      evict_id_in_lru = pair.first;
    }
  }
  if (evict_access_in_fifo != INT32_MAX) {
    lru_map_.erase(evict_id_in_fifo);
    *frame_id = evict_id_in_fifo;
    curr_evictable_size_--;
    curr_size_--;
    latch_.unlock();
    return true;
  }
  if (evict_access_in_lru != INT32_MAX) {
    lru_map_.erase(evict_id_in_lru);
    *frame_id = evict_id_in_lru;
    curr_evictable_size_--;
    curr_size_--;
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id <= (int)replacer_size_, "Invalid frame id");
  latch_.lock();
  auto it = lru_map_.find(frame_id);
  // find this frame
  if (it != lru_map_.end()) {
    it->second.access_count_++;
    it->second.last_access_time_ = current_timestamp_++;
    // if find this frame，the first_access_time must larger than 0
    latch_.unlock();
    return;
  }
  // not find this frame
  // if this map is full
  frame_id_t temp = 0;
  if (curr_size_ == replacer_size_ && !Evict(&temp)) {
    // if not have frame can evict， return
    latch_.unlock();
    return;
  }
  // create a new FeameInfo and emplace it in map
  FrameInfo fi;
  fi.first_access_time_ = current_timestamp_++;
  fi.access_count_++;
  fi.last_access_time_ = fi.first_access_time_;
  lru_map_.emplace(frame_id, fi);
  curr_size_++;
  curr_evictable_size_++;
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  auto it = lru_map_.find(frame_id);
  if (it != lru_map_.end()) {
    bool old_evictable = it->second.evictable_;
    it->second.evictable_ = set_evictable;
    if (old_evictable && !set_evictable) {
      curr_evictable_size_--;
    }
    if (!old_evictable && set_evictable) {
      curr_evictable_size_++;
    }
  }
  // if not found this frame， do nothing
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  auto it = lru_map_.find(frame_id);
  if (it != lru_map_.end()) {
    if (it->second.evictable_) {
      lru_map_.erase(frame_id);
      curr_evictable_size_--;
      curr_size_--;
    } else {
      latch_.unlock();
      throw std::invalid_argument("this frame not allow to remove");
    }
  }
  // If specified frame is not found，directly return from this function.
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_evictable_size_; }
}  // namespace bustub
