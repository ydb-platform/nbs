#include "caching_allocator.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/vector.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>
#include <util/system/yassert.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ICachingAllocator::TNode
{
    char* Data;
    TAtomic Refs;
    TNode* Next;
};

using TNode = ICachingAllocator::TNode;
using TBlock = ICachingAllocator::TBlock;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFreeList
{
private:
    TNode* Head = nullptr;

public:
    void Enqueue(TNode* node)
    {
        for (;;) {
            node->Next = AtomicGet(Head);

            if (AtomicCas(&Head, node, node->Next)) {
                break;
            }
        }
    }

    TNode* DequeueSingleConsumer()
    {
        for (;;) {
            auto head = AtomicGet(Head);

            if (!head) {
                break;
            }

            if (AtomicCas(&Head, head->Next, head)) {
                return head;
            }
        }

        return nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCachingAllocator: public ICachingAllocator
{
    struct TCurrentPage
    {
        TNode* Node;
        size_t Offset;
    };

private:
    IAllocator* const Upstream;

    const size_t PageSize;
    const size_t MaxPageCount;
    const size_t PageDropSize;

    TVector<TNode> Nodes;

    TCurrentPage CurrentPage = {};
    TFreeList FreeList;
    size_t PageCount = 0;

public:
    TCachingAllocator(
        IAllocator* upstream,
        size_t pageSize,
        size_t maxPageCount,
        size_t pageDropSize);

    ~TCachingAllocator();

    TBlock Allocate(size_t len) override;
    void Release(TBlock block) override;

private:
    void Deallocate(TNode* node);
    bool PickNewPage();
    TBlock AllocateFromCurrentPage(size_t bytesCount);
    TBlock Fallback(size_t bytesCount);
};

////////////////////////////////////////////////////////////////////////////////

TCachingAllocator::TCachingAllocator(
    IAllocator* upstream,
    size_t pageSize,
    size_t maxPageCount,
    size_t pageDropSize)
    : Upstream(upstream)
    , PageSize(pageSize)
    , MaxPageCount(maxPageCount)
    , PageDropSize(pageDropSize)
    , Nodes(maxPageCount, TNode())
{}

TCachingAllocator::~TCachingAllocator()
{
    for (size_t i = 0; i != PageCount; ++i) {
        Upstream->Release({Nodes[i].Data, PageSize});
    }
}

TBlock TCachingAllocator::Allocate(size_t bytesCount)
{
    if (bytesCount > PageSize) {
        return Fallback(bytesCount);
    }

    if (CurrentPage.Node) {
        const auto currentSize = PageSize - CurrentPage.Offset;

        if (currentSize >= bytesCount) {
            return AllocateFromCurrentPage(bytesCount);
        }

        if (currentSize > PageDropSize) {
            return Fallback(bytesCount);
        }
    }

    if (PickNewPage()) {
        return AllocateFromCurrentPage(bytesCount);
    }

    return Fallback(bytesCount);
}

void TCachingAllocator::Release(TBlock block)
{
    if (block.Node) {
        Deallocate(block.Node);
    } else {
        Upstream->Release({block.Data, block.Len});
    }
}

TBlock TCachingAllocator::Fallback(size_t bytesCount)
{
    const auto block = Upstream->Allocate(bytesCount);

    return {block.Data, block.Len, nullptr};
}

void TCachingAllocator::Deallocate(TNode* node)
{
    const auto refs = AtomicDecrement(node->Refs);
    Y_ABORT_UNLESS(refs >= 0);

    if (refs) {
        return;
    }

    FreeList.Enqueue(node);
}

bool TCachingAllocator::PickNewPage()
{
    if (CurrentPage.Node && AtomicDecrement(CurrentPage.Node->Refs) == 0) {
        // reset current page

        CurrentPage.Offset = 0;
        CurrentPage.Node->Refs = 1;

        return true;
    }

    CurrentPage = {};

    if (auto node = FreeList.DequeueSingleConsumer()) {
        node->Refs = 1;
        CurrentPage.Node = node;

        return true;
    }

    if (PageCount >= MaxPageCount) {
        return false;
    }

    // allocate new page

    TNode* node = &Nodes[PageCount++];

    node->Data = static_cast<char*>(Upstream->Allocate(PageSize).Data);
    node->Next = nullptr;
    node->Refs = 1;

    CurrentPage.Node = node;

    return true;
}

TBlock TCachingAllocator::AllocateFromCurrentPage(size_t bytesCount)
{
    TNode* node = CurrentPage.Node;

    AtomicIncrement(node->Refs);

    auto p = node->Data + CurrentPage.Offset;
    CurrentPage.Offset += bytesCount;

    return {p, bytesCount, node};
}

////////////////////////////////////////////////////////////////////////////////

class TSyncCachingAllocator final: public TCachingAllocator
{
private:
    TMutex AllocationLock;

public:
    using TCachingAllocator::TCachingAllocator;

    TBlock Allocate(size_t len) override
    {
        auto guard = Guard(AllocationLock);

        return TCachingAllocator::Allocate(len);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ICachingAllocatorPtr CreateCachingAllocator(
    IAllocator* upstream,
    size_t pageSize,
    size_t maxPageCount,
    size_t pageDropSize)
{
    return std::make_shared<TCachingAllocator>(
        upstream,
        pageSize,
        maxPageCount,
        pageDropSize);
}

ICachingAllocatorPtr CreateSyncCachingAllocator(
    IAllocator* upstream,
    size_t pageSize,
    size_t maxPageCount,
    size_t pageDropSize)
{
    return std::make_shared<TSyncCachingAllocator>(
        upstream,
        pageSize,
        maxPageCount,
        pageDropSize);
}

}   // namespace NCloud::NBlockStore
