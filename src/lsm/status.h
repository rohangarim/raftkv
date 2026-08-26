#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <variant>

namespace raftkv::lsm {

enum class Code : uint8_t {
  kOk = 0,
  kNotFound,
  kCorruption,
  kIoError,
  kInvalidArgument,
  kNotSupported,
};

// A small non-throwing error type. The storage engine is on the apply path,
// where an exception unwinding through half-written state is a worse outcome
// than an error code the caller is forced to look at.
class [[nodiscard]] Status {
 public:
  Status() = default;

  static Status Ok() { return {}; }
  static Status NotFound(std::string msg) { return Status(Code::kNotFound, std::move(msg)); }
  static Status Corruption(std::string msg) { return Status(Code::kCorruption, std::move(msg)); }
  static Status IoError(std::string msg) { return Status(Code::kIoError, std::move(msg)); }
  static Status InvalidArgument(std::string msg) {
    return Status(Code::kInvalidArgument, std::move(msg));
  }
  static Status NotSupported(std::string msg) {
    return Status(Code::kNotSupported, std::move(msg));
  }

  bool ok() const { return code_ == Code::kOk; }
  Code code() const { return code_; }
  const std::string& message() const { return message_; }

  std::string ToString() const;

 private:
  Status(Code code, std::string message) : code_(code), message_(std::move(message)) {}

  Code code_ = Code::kOk;
  std::string message_;
};

// Either a value or a Status. Deliberately not std::expected: this codebase
// targets compilers where <expected> availability is uneven, and the surface
// we need is small.
template <typename T>
class [[nodiscard]] Result {
 public:
  Result(T value) : value_(std::move(value)) {}  // NOLINT(google-explicit-constructor)
  Result(Status status) : value_(std::move(status)) {  // NOLINT(google-explicit-constructor)
  }

  bool ok() const { return std::holds_alternative<T>(value_); }
  explicit operator bool() const { return ok(); }

  T& operator*() { return std::get<T>(value_); }
  const T& operator*() const { return std::get<T>(value_); }
  T* operator->() { return &std::get<T>(value_); }

  T&& TakeValue() { return std::move(std::get<T>(value_)); }
  const Status& status() const { return std::get<Status>(value_); }

 private:
  std::variant<T, Status> value_;
};

#define RAFTKV_RETURN_IF_ERROR(expr)          \
  do {                                        \
    ::raftkv::lsm::Status s_ = (expr);        \
    if (!s_.ok()) {                           \
      return s_;                              \
    }                                         \
  } while (0)

}  // namespace raftkv::lsm
