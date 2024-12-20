//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

  enum class OperationType { FIND, INSERT, REMOVE };

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return leaf page
  auto GetLeafPage(const KeyType &key, Transaction *transaction, OperationType type) -> Page *;

  // return leaf for find
  auto GetLeafPageForFind(const KeyType &key) -> Page *;

  // return leaf for iterator
  auto GetLeafPageForIterator(const KeyType &key) -> Page *;

  // lock root
  void LockRoot(Transaction *transaction, OperationType type);

  // check safe page
  auto IsSafePage(BPlusTreePage *page, OperationType type) const -> bool;

  void LockPage(Page *page, Transaction *transaction, OperationType type);

  void UnlockAllPages(Transaction *transaction, OperationType type);

  auto GetPageFromSet(page_id_t page_id, Transaction *transaction) -> Page *;

  // split internel page or leaf page
  auto Split(BPlusTreePage *page) -> BPlusTreePage *;

  // after split, reset two node
  void InsertToParent(BPlusTreePage *old_page, BPlusTreePage *split_page, const KeyType &split_key,
                      Transaction *transaction);

  // steal node from the bro in left or right, or merge left or right
  void RedistributeOrMerge(BPlusTreePage *node, Transaction *transaction);

  // steal from left
  void RedistributeLeft(BPlusTreePage *left_node, BPlusTreePage *node, InternalPage *parent, int index);

  // steal from right
  void RedistributeRight(BPlusTreePage *right_node, BPlusTreePage *node, InternalPage *parent, int index);

  // merge left node or right node with parcent node
  void Merge(BPlusTreePage *des_node, BPlusTreePage *src_node, InternalPage *parent, int index,
             Transaction *transaction);

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  std::shared_mutex root_mutex_;
};

}  // namespace bustub
