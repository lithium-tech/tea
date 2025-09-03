#pragma once

#include <csignal>
#include <initializer_list>
#include <stdexcept>

namespace tea {

class SignalBlocker {
 public:
  SignalBlocker() {
    sigset_t signals_to_block;
    sigfillset(&signals_to_block);
    if (pthread_sigmask(SIG_BLOCK, &signals_to_block, &signal_set_to_restore_) != 0) {
      throw std::runtime_error("Failed to block signals");
    }
    need_to_restore_ = true;
  }

  ~SignalBlocker() { Restore(); }

  void Restore() {
    if (need_to_restore_) {
      need_to_restore_ = false;
      if (pthread_sigmask(SIG_SETMASK, &signal_set_to_restore_, nullptr) != 0) {
        throw std::runtime_error("Failed to restore signal mask");
      }
    }
  }

 private:
  sigset_t signal_set_to_restore_;
  bool need_to_restore_;
};

}  // namespace tea
