/**
 * index_iterator.cpp
 */
#include <cassert>
#include "common/config.h"
#include "storage/page/b_plus_tree_page.h"

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, LeafPage *leaf, int index)
    : bpm_(bpm), leaf_(leaf), index_(index) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (bpm_ != nullptr) {
    bpm_->UnpinPage(leaf_->GetPageId(), false);
  }
};  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return (leaf_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_->GetSize() - 1);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_->GetItem(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (leaf_ == nullptr) {
    return *this;
  }
  if (leaf_->GetNextPageId() != INVALID_PAGE_ID && index_ == leaf_->GetSize() - 1) {
    page_id_t next_page_id = leaf_->GetNextPageId();
    bpm_->UnpinPage(leaf_->GetPageId(), false);
    Page *next_page = bpm_->FetchPage(next_page_id);
    auto *next = reinterpret_cast<LeafPage *>(next_page->GetData());
    leaf_ = next;
    index_ = 0;
  } else {
    index_++;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
