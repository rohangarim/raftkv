#pragma once

// Structured-ish logging with a compile-time-cheap runtime level check.
//
// Rule for this repo: nothing on the replication hot path (Ready loop, append,
// apply) logs at anything above kDebug, and every call site is guarded so that
// a disabled level costs one relaxed atomic load and a predictable branch --
// no argument evaluation, no allocation, no formatting.

#include <atomic>
#include <cstdio>
#include <string_view>

namespace raftkv {

enum class LogLevel : int {
  kTrace = 0,
  kDebug = 1,
  kInfo = 2,
  kWarn = 3,
  kError = 4,
  kOff = 5,
};

namespace detail {
inline std::atomic<int>& LevelCell() {
  static std::atomic<int> cell{static_cast<int>(LogLevel::kInfo)};
  return cell;
}
}  // namespace detail

inline void SetLogLevel(LogLevel level) {
  detail::LevelCell().store(static_cast<int>(level), std::memory_order_relaxed);
}

inline LogLevel GetLogLevel() {
  return static_cast<LogLevel>(detail::LevelCell().load(std::memory_order_relaxed));
}

inline bool LogEnabled(LogLevel level) {
  return static_cast<int>(level) >= detail::LevelCell().load(std::memory_order_relaxed);
}

constexpr std::string_view LevelName(LogLevel level) {
  switch (level) {
    case LogLevel::kTrace: return "TRACE";
    case LogLevel::kDebug: return "DEBUG";
    case LogLevel::kInfo:  return "INFO";
    case LogLevel::kWarn:  return "WARN";
    case LogLevel::kError: return "ERROR";
    case LogLevel::kOff:   return "OFF";
  }
  return "?";
}

// Deliberately printf-shaped: format-string checking at compile time, no
// iostream, no std::endl, no per-call flush.
void LogAt(LogLevel level, const char* file, int line, const char* fmt, ...)
    __attribute__((format(printf, 4, 5)));

}  // namespace raftkv

#define RAFTKV_LOG(level, ...)                                                        \
  do {                                                                                \
    if (::raftkv::LogEnabled(level)) {                                                \
      ::raftkv::LogAt((level), __FILE__, __LINE__, __VA_ARGS__);                       \
    }                                                                                 \
  } while (0)

#define LOG_TRACE(...) RAFTKV_LOG(::raftkv::LogLevel::kTrace, __VA_ARGS__)
#define LOG_DEBUG(...) RAFTKV_LOG(::raftkv::LogLevel::kDebug, __VA_ARGS__)
#define LOG_INFO(...)  RAFTKV_LOG(::raftkv::LogLevel::kInfo, __VA_ARGS__)
#define LOG_WARN(...)  RAFTKV_LOG(::raftkv::LogLevel::kWarn, __VA_ARGS__)
#define LOG_ERROR(...) RAFTKV_LOG(::raftkv::LogLevel::kError, __VA_ARGS__)
