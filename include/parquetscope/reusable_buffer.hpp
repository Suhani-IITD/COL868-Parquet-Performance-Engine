#pragma once
#include <memory>
#include <span>

namespace parquetscope {
// Uninitialized storage for trivial values; every element must be written before reading.
template<class T>
class ReusableBuffer {
public:
    // Grow only when necessary and return a view of exactly count elements.
    std::span<T> prepare(size_t count) {
        if (count > capacity_) {
            storage_ = std::make_unique_for_overwrite<T[]>(count);
            capacity_ = count;
        }
        return {storage_.get(), count};
    }
    // Return retained capacity, used to enforce scan workspace limits.
    size_t capacity() const { return capacity_; }
    T* data() { return storage_.get(); }
    const T* data() const { return storage_.get(); }
private:
    std::unique_ptr<T[]> storage_;
    size_t capacity_ = 0;
};
} // namespace parquetscope
