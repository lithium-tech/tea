#include <chrono>
#include <cstdint>
#include <memory>
#include <new>
#include <ratio>
#include <stdexcept>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

#include "tea/util/cancel.h"
#include "tea/util/thread_pool.h"

namespace tea {
namespace {

TEST(CancelToken, Trivial) {
  CancelToken token;
  EXPECT_FALSE(token.IsCancelled());
  token.Cancel();
  EXPECT_TRUE(token.IsCancelled());
}

TEST(CancelToken, MultipleThreads) {
  CancelToken token;

  std::atomic<uint64_t> a = 0;
  uint64_t b = 0;
  std::thread x([&]() {
    while (!token.IsCancelled()) {
      a.fetch_add(1);
    }
    ++b;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  ASSERT_GT(a, 100);

  token.Cancel();
  uint64_t old_a = a.load();

  x.join();
  uint64_t new_a = a.load();
  ASSERT_EQ(b, 1);
  ASSERT_LE(new_a, old_a + 1);
}

TEST(CancelToken, WaitFor) {
  CancelToken token;

  auto start = std::chrono::steady_clock::now().time_since_epoch();

  bool a = false;
  bool b = false;
  bool c = false;
  std::thread x([&]() {
    a = token.WaitFor(std::chrono::milliseconds(10));
    b = token.WaitFor(std::chrono::milliseconds(1000));
    c = token.WaitFor(std::chrono::milliseconds(10));
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  token.Cancel();

  x.join();

  EXPECT_FALSE(a);
  EXPECT_TRUE(b);
  EXPECT_TRUE(c);

  auto end = std::chrono::steady_clock::now().time_since_epoch();

  auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(start - end).count();
  EXPECT_LE(total_duration, 900);
}

}  // namespace
}  // namespace tea
