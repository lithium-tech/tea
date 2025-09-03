#include "tea/util/lru_cache.h"

#include <cstdint>
#include <memory>
#include <new>
#include <optional>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace tea {
namespace {

TEST(LRUCache, Simple) {
  auto cache = LRUCache<int, int>(2);
  EXPECT_TRUE(cache.PushItem(1, 2));
  EXPECT_EQ(cache.GetValue(1), 2);

  cache.PushItem(3, 4);
  cache.PushItem(5, 6);

  EXPECT_EQ(cache.GetValue(1), std::nullopt);
  EXPECT_EQ(cache.GetValue(3), 4);
  EXPECT_EQ(cache.GetValue(5), 6);
}

TEST(LRUCache, Calculation) {
  auto cache = LRUCache<int, int>(2);
  EXPECT_TRUE(cache.PushItem(1, 2));
  EXPECT_EQ(cache.GetValue(1), 2);

  EXPECT_EQ(2, cache.GetValueOrCalculate(1, []() -> int { return 3; }));

  EXPECT_EQ(5, cache.GetValueOrCalculate(4, []() -> int { return 5; }));

  EXPECT_EQ(5, cache.GetValueOrCalculate(4, []() -> int { throw std::runtime_error("some error"); }));
}

TEST(LRUCache, Simple1) {
  auto cache = LRUCache<int, int>(2);
  EXPECT_TRUE(cache.PushItem(1, 10));
  EXPECT_TRUE(cache.PushItem(2, 20));
  EXPECT_TRUE(cache.PushItem(1, 15));

  EXPECT_EQ(cache.GetValue(1), 15);
  EXPECT_EQ(cache.GetValue(2), 20);
}

TEST(LRUCache, Simple2) {
  auto cache = LRUCache<int, int>(2);
  EXPECT_TRUE(cache.PushItem(1, 10));
  EXPECT_TRUE(cache.PushItem(2, 20));

  EXPECT_TRUE(cache.PushItem(3, 30));
  EXPECT_EQ(cache.GetValue(1), std::nullopt);

  EXPECT_TRUE(cache.PushItem(1, 40));
  EXPECT_EQ(cache.GetValue(1), 40);
  EXPECT_EQ(cache.GetValue(2), std::nullopt);
  EXPECT_EQ(cache.GetValue(3), 30);
}

TEST(LRUCache, LargeCapacityAndHighVolume) {
  auto cache = LRUCache<int, int>(1000);
  for (int i = 0; i < 1500; ++i) {
    cache.PushItem(i, i * 10);
  }

  for (int i = 0; i < 500; ++i) {
    EXPECT_EQ(cache.GetValue(i), std::nullopt);
  }

  for (int i = 500; i < 1500; ++i) {
    EXPECT_EQ(cache.GetValue(i), i * 10);
  }
}

TEST(LRUCache, NonTrivialKeyValueTypes) {
  auto cache = LRUCache<std::string, std::shared_ptr<std::string>>(1);
  EXPECT_TRUE(cache.PushItem("a", nullptr));
  EXPECT_TRUE(cache.PushItem("b", std::make_shared<std::string>("bc")));

  EXPECT_EQ(cache.GetValue("a"), std::nullopt);
  EXPECT_EQ(*(*cache.GetValue("b")), "bc");
}

}  // namespace
}  // namespace tea
