#pragma once

// A linearizability checker for a set of independent registers, after Wing &
// Gong with Lowe's memoisation. Given a history of operations with real-time
// invoke and return stamps, it searches for one total order that respects
// real time and that a single-copy register could have produced.
//
// Operations whose outcome is unknown (the client timed out, so the write may
// or may not have been applied) are "info": they never return, so they never
// constrain anything, and the search may place them anywhere after their invoke
// or leave them out.
//
// Keys are independent registers, so each key is checked alone. Writes use
// unique values, which keeps the state space small.

#include <algorithm>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

namespace raftkv::testing {

enum class RegOp { kPut, kGet, kCas };

struct HistoryOp {
  RegOp kind = RegOp::kPut;
  std::string key;
  // Put: the value written. Cas: the new value.
  std::string value;
  // Cas: the value that must be present (nullopt = absent).
  std::optional<std::string> expected;

  // False when the outcome is unknown.
  bool completed = true;
  // Get: what was read. Cas (failed): what was found.
  std::optional<std::string> observed;
  // Cas: whether the swap happened.
  bool applied = false;

  uint64_t invoke = 0;
  uint64_t ret = std::numeric_limits<uint64_t>::max();
  int thread = 0;
};

struct CheckResult {
  bool linearizable = true;
  // When not: the key whose history has no valid order, and its operations.
  std::string failing_key;
  std::vector<HistoryOp> failing_history;
  // The search gave up on a key (state budget exhausted); not a verdict.
  bool inconclusive = false;
};

namespace lin_detail {

// State = which value the register holds, as an index into `values` (-1 for
// absent).
class KeyChecker {
 public:
  explicit KeyChecker(std::vector<HistoryOp> ops, size_t budget) : ops_(std::move(ops)), budget_(budget) {
    // Info gets say nothing; drop them.
    ops_.erase(std::remove_if(ops_.begin(), ops_.end(),
                              [](const HistoryOp& o) { return !o.completed && o.kind == RegOp::kGet; }),
               ops_.end());
    std::stable_sort(ops_.begin(), ops_.end(), [](const HistoryOp& a, const HistoryOp& b) {
      // Completed first, then by invoke; the search tries them in this order.
      if (a.completed != b.completed) return a.completed;
      return a.invoke < b.invoke;
    });
    done_.assign(ops_.size(), false);
    for (const auto& o : ops_) {
      if (o.completed) ++remaining_;
    }
  }

  // 1 = linearizable, 0 = not, -1 = out of budget.
  int Run() { return Search(-1) ? 1 : (exhausted_ ? -1 : 0); }

 private:
  int Intern(const std::optional<std::string>& v) {
    if (!v) return -1;
    auto it = ids_.find(*v);
    if (it != ids_.end()) return it->second;
    const int id = static_cast<int>(ids_.size());
    ids_.emplace(*v, id);
    return id;
  }

  // Can `op` be linearized when the register holds `state`? If so, the state
  // afterwards.
  bool Step(const HistoryOp& op, int state, int* next) {
    switch (op.kind) {
      case RegOp::kPut:
        *next = Intern(op.value);
        return true;
      case RegOp::kGet:
        *next = state;
        return Intern(op.observed) == state;
      case RegOp::kCas: {
        const int expected = Intern(op.expected);
        if (!op.completed) {
          // Unknown outcome: it linearizes as a success if it could have.
          if (state != expected) return false;
          *next = Intern(op.value);
          return true;
        }
        if (op.applied) {
          if (state != expected) return false;
          *next = Intern(op.value);
          return true;
        }
        *next = state;
        return state != expected && Intern(op.observed) == state;
      }
    }
    return false;
  }

  std::string MemoKey(int state) const {
    std::string k;
    k.reserve(done_.size() / 8 + 8);
    uint8_t byte = 0;
    for (size_t i = 0; i < done_.size(); ++i) {
      byte = static_cast<uint8_t>(byte | (done_[i] ? 1U << (i % 8) : 0U));
      if (i % 8 == 7) {
        k.push_back(static_cast<char>(byte));
        byte = 0;
      }
    }
    k.push_back(static_cast<char>(byte));
    k.append(reinterpret_cast<const char*>(&state), sizeof(state));
    return k;
  }

  bool Search(int state) {
    if (remaining_ == 0) return true;
    if (++steps_ > budget_) {
      exhausted_ = true;
      return false;
    }
    const std::string memo = MemoKey(state);
    if (failed_.count(memo) != 0) return false;

    // An operation can go next only if it was invoked before every other
    // pending completed operation returned; otherwise that one must come first.
    uint64_t earliest_return = std::numeric_limits<uint64_t>::max();
    for (size_t i = 0; i < ops_.size(); ++i) {
      if (!done_[i] && ops_[i].completed) earliest_return = std::min(earliest_return, ops_[i].ret);
    }
    for (size_t i = 0; i < ops_.size(); ++i) {
      if (done_[i] || ops_[i].invoke >= earliest_return) continue;
      int next = 0;
      if (!Step(ops_[i], state, &next)) continue;
      done_[i] = true;
      if (ops_[i].completed) --remaining_;
      const bool ok = Search(next);
      done_[i] = false;
      if (ops_[i].completed) ++remaining_;
      if (ok) return true;
      if (exhausted_) return false;
    }
    failed_.insert(memo);
    return false;
  }

  std::vector<HistoryOp> ops_;
  std::vector<bool> done_;
  size_t remaining_ = 0;
  std::map<std::string, int> ids_;
  std::unordered_set<std::string> failed_;
  size_t budget_;
  size_t steps_ = 0;
  bool exhausted_ = false;
};

}  // namespace lin_detail

// `budget` bounds the search steps per key.
inline CheckResult CheckLinearizable(const std::vector<HistoryOp>& history, size_t budget = 5'000'000) {
  std::map<std::string, std::vector<HistoryOp>> by_key;
  for (const auto& op : history) by_key[op.key].push_back(op);

  CheckResult result;
  for (auto& [key, ops] : by_key) {
    lin_detail::KeyChecker checker(ops, budget);
    const int verdict = checker.Run();
    if (verdict == 0) {
      result.linearizable = false;
      result.failing_key = key;
      result.failing_history = ops;
      return result;
    }
    if (verdict < 0) result.inconclusive = true;
  }
  return result;
}

}  // namespace raftkv::testing
