#include <cstddef>
#include <string>
#include <type_traits>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"
#include "storage/page/page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key, Transaction *transaction, OperationType type) -> Page * {
  // get the page of root node
  Page *page_ptr = buffer_pool_manager_->FetchPage(GetRootPageId());
  LockPage(page_ptr, transaction, type);
  auto *page_data_ptr = reinterpret_cast<BPlusTreePage *>(page_ptr->GetData());
  // get left node
  while (!page_data_ptr->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(page_data_ptr);
    // get child node id
    auto page_id = internal->Lookup(key, comparator_);
    page_ptr = buffer_pool_manager_->FetchPage(page_id);
    LockPage(page_ptr, transaction, type);
    page_data_ptr = reinterpret_cast<BPlusTreePage *>(page_ptr->GetData());
  }
  return page_ptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPageForFind(const KeyType &key) -> Page * {
  // get the page of root node
  Page *page_ptr = buffer_pool_manager_->FetchPage(GetRootPageId());
  page_ptr->RLatch();
  root_mutex_.unlock_shared();
  auto *page_data_ptr = reinterpret_cast<BPlusTreePage *>(page_ptr->GetData());
  // get left node
  while (!page_data_ptr->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(page_data_ptr);
    // get child node id
    auto page_id = internal->Lookup(key, comparator_);
    auto *child_page_ptr = buffer_pool_manager_->FetchPage(page_id);
    child_page_ptr->RLatch();
    page_ptr->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
    page_ptr = child_page_ptr;
    page_data_ptr = reinterpret_cast<BPlusTreePage *>(page_ptr->GetData());
  }
  return page_ptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPageForIterator(const KeyType &key) -> Page * {
  // get the page's data of root node
  Page *page_ptr = buffer_pool_manager_->FetchPage(root_page_id_);
  auto page_data_ptr = reinterpret_cast<BPlusTreePage *>(page_ptr->GetData());
  // get left node
  while (!page_data_ptr->IsLeafPage()) {
    auto internal_page_ptr = reinterpret_cast<InternalPage *>(page_data_ptr);
    // get child node id
    auto page_id = internal_page_ptr->Lookup(key, comparator_);
    page_ptr = buffer_pool_manager_->FetchPage(page_id);
    page_data_ptr = reinterpret_cast<BPlusTreePage *>(page_ptr->GetData());
    // unpin the parent node page
    buffer_pool_manager_->UnpinPage(page_data_ptr->GetParentPageId(), false);
  }
  return page_ptr;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LockPage(Page *page, Transaction *transaction, OperationType type) {
  page->WLatch();
  if (IsSafePage(reinterpret_cast<BPlusTreePage *>(page->GetData()), type)) {
    UnlockAllPages(transaction, type);
  }
  transaction->AddIntoPageSet(page);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockAllPages(Transaction *transaction, OperationType type) {
  auto page_set = transaction->GetPageSet();
  while (!page_set->empty()) {
    auto *cur_page = page_set->front();
    if (cur_page == nullptr) {
      root_mutex_.unlock();
      page_set->pop_front();
      continue;
    }
    cur_page->WUnlatch();
    if (cur_page->IsDirty()) {
      buffer_pool_manager_->FlushPage(cur_page->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
    auto delete_page_set = transaction->GetDeletedPageSet();
    if (delete_page_set->count(cur_page->GetPageId()) != 0) {
      buffer_pool_manager_->DeletePage(cur_page->GetPageId());
    }
    page_set->pop_front();
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafePage(BPlusTreePage *page, OperationType type) const -> bool {
  if (type == OperationType::INSERT) {
    if (page->IsLeafPage()) {
      return page->GetSize() < page->GetMaxSize() - 1;
    }
    return page->GetSize() < page->GetMaxSize();
  }
  // Remove
  if (page->IsRootPage()) {
    return page->GetSize() > (page->IsLeafPage() ? 1 : 2);
  }
  return page->GetSize() > page->GetMinSize();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LockRoot(Transaction *transaction, OperationType type) {
  if (type == OperationType::FIND) {
    root_mutex_.lock_shared();
  } else {
    root_mutex_.lock();
    transaction->AddIntoPageSet(nullptr);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetPageFromSet(page_id_t page_id, Transaction *transaction) -> Page * {
  auto page_set = transaction->GetPageSet();
  for (auto *page : *(page_set)) {
    if (page != nullptr && page->GetPageId() == page_id) {
      return page;
    }
  }
  return nullptr;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // get the page's data of root node
  if (root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  LockRoot(transaction, BPlusTree::OperationType::FIND);
  Page *page_ptr = GetLeafPageForFind(key);
  auto leaf_page_ptr = reinterpret_cast<LeafPage *>(page_ptr->GetData());
  ValueType value;
  bool is_exist = leaf_page_ptr->Lookup(key, value, comparator_);
  page_ptr->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), false);
  if (is_exist) {
    result->push_back(value);
  }
  return is_exist;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  LockRoot(transaction, BPlusTree::OperationType::INSERT);
  // if tree is empty, create a root node
  if (IsEmpty()) {
    Page *page_ptr = buffer_pool_manager_->NewPage(&root_page_id_);
    if (page_ptr == nullptr) {
      UnlockAllPages(transaction, BPlusTree::OperationType::INSERT);
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Allocate new page failed.");
    }
    auto *leaf_page_ptr = reinterpret_cast<LeafPage *>(page_ptr->GetData());
    leaf_page_ptr->Init(GetRootPageId(), INVALID_PAGE_ID, leaf_max_size_);
    leaf_page_ptr->Insert(key, value, comparator_);
    UpdateRootPageId(1);
    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
    UnlockAllPages(transaction, BPlusTree::OperationType::INSERT);
    return true;
  }
  Page *page_ptr = GetLeafPage(key, transaction, BPlusTree::OperationType::INSERT);
  auto *leaf_page_ptr = reinterpret_cast<LeafPage *>(page_ptr->GetData());
  int old_size = leaf_page_ptr->GetSize();
  int size = leaf_page_ptr->Insert(key, value, comparator_);
  if (size == old_size) {
    UnlockAllPages(transaction, BPlusTree::OperationType::INSERT);
    return false;
  }
  if (size < leaf_max_size_) {
    buffer_pool_manager_->FlushPage(leaf_page_ptr->GetPageId());
    UnlockAllPages(transaction, BPlusTree::OperationType::INSERT);
    return true;
  }
  // split
  auto *new_leaf_page_ptr = reinterpret_cast<LeafPage *>(Split(leaf_page_ptr));
  new_leaf_page_ptr->SetNextPageId(leaf_page_ptr->GetNextPageId());
  leaf_page_ptr->SetNextPageId(new_leaf_page_ptr->GetPageId());
  InsertToParent(leaf_page_ptr, new_leaf_page_ptr, new_leaf_page_ptr->KeyAt(0), transaction);
  buffer_pool_manager_->FlushPage(leaf_page_ptr->GetPageId());
  buffer_pool_manager_->UnpinPage(new_leaf_page_ptr->GetPageId(), true);
  UnlockAllPages(transaction, BPlusTree::OperationType::INSERT);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(BPlusTreePage *page) -> BPlusTreePage * {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "New page failed.");
  }
  if (page->IsLeafPage()) {
    auto *leaf_page = reinterpret_cast<LeafPage *>(page);
    auto *new_leaf = reinterpret_cast<LeafPage *>(new_page->GetData());
    new_leaf->Init(new_page_id, leaf_page->GetParentPageId(), leaf_max_size_);
    leaf_page->MoveHalfTo(new_leaf);
  } else {
    // for internel node
    auto *internal_page = reinterpret_cast<InternalPage *>(page);
    auto *new_internel = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_internel->Init(new_page_id, internal_page->GetParentPageId(), internal_max_size_);
    internal_page->MoveHalfTo(new_internel, buffer_pool_manager_);
  }
  return reinterpret_cast<BPlusTreePage *>(new_page->GetData());  // switch new_leaf is ok too
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertToParent(BPlusTreePage *old_page, BPlusTreePage *split_page, const KeyType &split_key,
                                    Transaction *transaction) {
  // if oldpage is root, create a new root replace it
  if (old_page->IsRootPage()) {
    Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
    auto *root = reinterpret_cast<InternalPage *>(page->GetData());
    root->Init(GetRootPageId(), INVALID_PAGE_ID, internal_max_size_);
    root->SetKeyAt(1, split_key);
    root->SetValueAt(1, split_page->GetPageId());
    root->SetValueAt(0, old_page->GetPageId());
    root->SetSize(2);
    old_page->SetParentPageId(GetRootPageId());
    split_page->SetParentPageId(GetRootPageId());
    UpdateRootPageId(0);
    buffer_pool_manager_->FetchPage(GetRootPageId());
    buffer_pool_manager_->UnpinPage(GetRootPageId(), false);
    return;
  }
  // if parent's size is not touch max_size
  page_id_t parent_id = old_page->GetParentPageId();
  // Page *parent_page = GetPageFromSet(parent_id, transaction);
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  if (parent->GetSize() < internal_max_size_) {
    parent->InsertNodeAfter(split_page->GetPageId(), split_key, old_page->GetPageId());
    buffer_pool_manager_->FlushPage(parent_id);
    buffer_pool_manager_->UnpinPage(parent_id, false);
    return;
  }

  // if parent's size touch max_size
  parent->InsertNodeAfter(split_page->GetPageId(), split_key, old_page->GetPageId());
  auto *new_parent_page = reinterpret_cast<InternalPage *>(Split(parent));
  InsertToParent(parent, new_parent_page, new_parent_page->KeyAt(0), transaction);
  buffer_pool_manager_->FlushPage(parent_id);
  buffer_pool_manager_->UnpinPage(parent_id, false);
  buffer_pool_manager_->UnpinPage(new_parent_page->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  LockRoot(transaction, BPlusTree::OperationType::REMOVE);
  Page *page = GetLeafPage(key, transaction, BPlusTree::OperationType::REMOVE);
  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  // delete not success
  if (!leaf->Remove(key, comparator_)) {
    UnlockAllPages(transaction, BPlusTree::OperationType::REMOVE);
    return;
  }
  // if size is greater than min_size
  if (leaf->GetSize() >= leaf->GetMinSize()) {
    buffer_pool_manager_->FlushPage(leaf->GetPageId());
    UnlockAllPages(transaction, BPlusTree::OperationType::REMOVE);
    return;
  }
  // steal node from the bro in left or right, or merge left or right
  RedistributeOrMerge(reinterpret_cast<BPlusTreePage *>(leaf), transaction);
  UnlockAllPages(transaction, BPlusTree::OperationType::REMOVE);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeOrMerge(BPlusTreePage *node, Transaction *transaction) {
  // if this node is root, return
  if (node->IsRootPage()) {
    if (node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
    }
    return;
  }

  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  // Page *parent_page = GetPageFromSet(node->GetParentPageId(), transaction);
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int index = parent->ValueIndex(node->GetPageId());
  //  steal left node
  if (index > 0) {
    int left_subling_id = parent->ValueAt(index - 1);
    Page *left_subling_page = buffer_pool_manager_->FetchPage(left_subling_id);
    left_subling_page->WLatch();
    auto *left_subling = reinterpret_cast<BPlusTreePage *>(left_subling_page->GetData());
    if (left_subling->GetSize() > left_subling->GetMinSize()) {
      RedistributeLeft(left_subling, node, parent, index);
      buffer_pool_manager_->FlushPage(node->GetPageId());
      buffer_pool_manager_->FlushPage(parent->GetPageId());
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
      left_subling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_subling_id, true);
      return;
    }
    left_subling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_subling_id, false);
  }
  //  steal right node
  if (index < parent->GetSize() - 1) {
    int right_subling_id = parent->ValueAt(index + 1);
    Page *right_subling_page = buffer_pool_manager_->FetchPage(right_subling_id);
    right_subling_page->WLatch();
    auto *right_subling = reinterpret_cast<BPlusTreePage *>(right_subling_page->GetData());
    if (right_subling->GetSize() > right_subling->GetMinSize()) {
      RedistributeRight(right_subling, node, parent, index + 1);
      buffer_pool_manager_->FlushPage(node->GetPageId());
      buffer_pool_manager_->FlushPage(parent->GetPageId());
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
      right_subling_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(right_subling_id, true);
      return;
    }
    right_subling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_subling_id, false);
  }

  // merge left node
  if (index > 0) {
    int left_subling_id = parent->ValueAt(index - 1);
    auto *left_subling_page = buffer_pool_manager_->FetchPage(left_subling_id);
    left_subling_page->WLatch();
    auto *left_subling = reinterpret_cast<BPlusTreePage *>(left_subling_page);
    Merge(left_subling, node, parent, index, transaction);
    transaction->AddIntoDeletedPageSet(node->GetPageId());
    buffer_pool_manager_->FlushPage(parent->GetPageId());
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
    buffer_pool_manager_->FlushPage(node->GetPageId());
    left_subling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_subling_id, true);
    return;
  }
  // merge right node
  if (index < parent->GetSize() - 1) {
    int right_subling_id = parent->ValueAt(index + 1);
    auto *right_subling_page = buffer_pool_manager_->FetchPage(right_subling_id);
    right_subling_page->WLatch();
    auto *right_subling = reinterpret_cast<BPlusTreePage *>(right_subling_page);
    Merge(node, right_subling, parent, index + 1, transaction);
    buffer_pool_manager_->FlushPage(parent->GetPageId());
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
    buffer_pool_manager_->FlushPage(node->GetPageId());
    right_subling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_subling_id, true);
    buffer_pool_manager_->DeletePage(right_subling_id);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(BPlusTreePage *des_node, BPlusTreePage *src_node, InternalPage *parent, int index,
                           Transaction *transaction) {
  if (des_node->IsLeafPage()) {
    auto *des_page = reinterpret_cast<LeafPage *>(des_node);
    auto *src_page = reinterpret_cast<LeafPage *>(src_node);
    src_page->MoveAllto(des_page);
  } else {
    auto *des_page = reinterpret_cast<InternalPage *>(des_node);
    auto *src_page = reinterpret_cast<InternalPage *>(src_node);
    src_page->SetKeyAt(0, parent->KeyAt(index));
    src_page->MoveALLTo(des_page, buffer_pool_manager_);
  }
  parent->Remove(index);
  if (parent->GetSize() < parent->GetMinSize()) {
    // switch root
    if (parent->IsRootPage() && parent->GetSize() == 1) {
      transaction->AddIntoDeletedPageSet(parent->GetPageId());
      root_page_id_ = parent->ValueAt(0);
      auto *des_page = reinterpret_cast<InternalPage *>(des_node);
      des_page->SetParentPageId(INVALID_PAGE_ID);
      parent->SetSize(0);
      UpdateRootPageId(0);
      return;
    }
    RedistributeOrMerge(parent, transaction);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeLeft(BPlusTreePage *left_node, BPlusTreePage *node, InternalPage *parent, int index) {
  KeyType key;
  if (left_node->IsLeafPage()) {
    auto *left_subling = reinterpret_cast<LeafPage *>(left_node);
    auto *parsent = reinterpret_cast<LeafPage *>(node);
    int left_index = left_subling->GetSize() - 1;
    key = left_subling->KeyAt(left_index);

    parsent->Insert(key, left_subling->ValueAt(left_index), comparator_);
    left_subling->IncreaseSize(-1);
  } else {
    auto *left_subling = reinterpret_cast<InternalPage *>(left_node);
    auto *parsent = reinterpret_cast<InternalPage *>(node);
    int leaf_index = left_subling->GetSize() - 1;
    key = left_subling->KeyAt(leaf_index);
    parsent->SetKeyAt(0, parent->KeyAt(index));
    parsent->InsertToStart(key, left_subling->ValueAt(leaf_index), buffer_pool_manager_);
    left_subling->IncreaseSize(-1);
  }
  parent->SetKeyAt(index, key);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeRight(BPlusTreePage *right_node, BPlusTreePage *node, InternalPage *parent,
                                       int index) {
  KeyType key;
  if (right_node->IsLeafPage()) {
    auto *right_subling = reinterpret_cast<LeafPage *>(right_node);
    auto *parsent = reinterpret_cast<LeafPage *>(node);
    int right_index = 0;
    key = right_subling->KeyAt(right_index);
    parsent->Insert(key, right_subling->ValueAt(right_index), comparator_);
    right_subling->Remove(key, comparator_);
  } else {
    auto *right_subling = reinterpret_cast<InternalPage *>(right_node);
    auto *parsent = reinterpret_cast<InternalPage *>(node);
    right_subling->SetKeyAt(0, parent->KeyAt(index));
    key = right_subling->KeyAt(1);
    parsent->InsertToEnd(right_subling->KeyAt(0), right_subling->ValueAt(0), buffer_pool_manager_);
    right_subling->Remove(0);
  }
  parent->SetKeyAt(index, key);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(nullptr, nullptr, 0);
  }
  Page *page = buffer_pool_manager_->FetchPage(GetRootPageId());
  auto page_ptr = reinterpret_cast<BPlusTreePage *>(page->GetData());
  // get left node
  while (!page_ptr->IsLeafPage()) {
    auto internal_page = reinterpret_cast<InternalPage *>(page_ptr);
    // get child node id
    auto page_id = internal_page->ValueAt(0);
    page = buffer_pool_manager_->FetchPage(page_id);
    page_ptr = reinterpret_cast<BPlusTreePage *>(page->GetData());
    // unpin the parent node page
    buffer_pool_manager_->UnpinPage(page_ptr->GetParentPageId(), false);
  }
  auto *leaf_page_ptr = reinterpret_cast<LeafPage *>(page_ptr);
  INDEXITERATOR_TYPE iterator_begin(buffer_pool_manager_, leaf_page_ptr, 0);
  return iterator_begin;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(nullptr, nullptr, 0);
  }
  Page *page_ptr = GetLeafPageForIterator(key);
  auto *leaf_page_ptr = reinterpret_cast<LeafPage *>(page_ptr->GetData());
  int index = leaf_page_ptr->KeyIndex(key, comparator_);
  INDEXITERATOR_TYPE iterator_begin_key(buffer_pool_manager_, leaf_page_ptr, index);
  return iterator_begin_key;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(nullptr, nullptr, 0);
  }
  Page *page = buffer_pool_manager_->FetchPage(GetRootPageId());
  auto page_ptr = reinterpret_cast<BPlusTreePage *>(page->GetData());
  // get left node
  while (!page_ptr->IsLeafPage()) {
    auto internal_page = reinterpret_cast<InternalPage *>(page_ptr);
    // get child node id
    auto page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    page = buffer_pool_manager_->FetchPage(page_id);
    page_ptr = reinterpret_cast<BPlusTreePage *>(page->GetData());
    // unpin the parent node page
    buffer_pool_manager_->UnpinPage(page_ptr->GetParentPageId(), false);
  }
  auto *leaf_page_ptr = reinterpret_cast<LeafPage *>(page_ptr);
  INDEXITERATOR_TYPE iterator_end(buffer_pool_manager_, leaf_page_ptr, leaf_page_ptr->GetSize());
  return iterator_end;
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
