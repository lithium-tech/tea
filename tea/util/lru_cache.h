#pragma once

#include <functional>
#include <list>
#include <optional>
#include <unordered_map>
#include <utility>

namespace tea {

template <typename Key, typename Value>
class LRUCache {
 public:
  explicit LRUCache(size_t cache_limit) : cache_limit_(cache_limit) {}

  LRUCache(const LRUCache&) = delete;
  LRUCache& operator=(const LRUCache&) = delete;
  LRUCache(LRUCache&&) = delete;
  LRUCache& operator=(LRUCache&&) = delete;

  bool PushItem(const Key& key, Value value) {
    auto it = cache_map_.find(key);

    if (it != cache_map_.end()) {
      it->second->second = std::move(value);
      cache_items_.splice(cache_items_.begin(), cache_items_, it->second);
      return true;
    }

    auto [map_iter, inserted] =
        cache_map_.emplace(std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple());
    if (inserted) {
      try {
        cache_items_.emplace_front(key, std::move(value));
        map_iter->second = cache_items_.begin();
      } catch (...) {
        cache_map_.erase(key);
        throw;
      }
    } else {
      return false;
    }

    if (cache_items_.size() > cache_limit_) {
      auto last = cache_items_.rbegin();
      cache_map_.erase(last->first);
      cache_items_.pop_back();
    }
    return true;
  }

  std::optional<Value> GetValue(const Key& key) {
    auto it = cache_map_.find(key);
    if (it == cache_map_.end()) {
      return std::nullopt;
    }

    cache_items_.splice(cache_items_.begin(), cache_items_, it->second);
    return it->second->second;
  }

  Value GetValueOrCalculate(const Key& key, const std::function<Value()>& calc_value_) {
    if (auto maybe_value = GetValue(key); maybe_value.has_value()) {
      return *maybe_value;
    }
    auto value = calc_value_();
    PushItem(key, value);
    return value;
  }

 private:
  size_t cache_limit_;
  std::list<std::pair<Key, Value>> cache_items_;
  std::unordered_map<Key, typename std::list<std::pair<Key, Value>>::iterator> cache_map_;
};

}  // namespace tea
