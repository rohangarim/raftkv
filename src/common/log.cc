#include "common/log.h"

#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <ctime>

namespace raftkv {
namespace {

const char* Basename(const char* path) {
  const char* slash = std::strrchr(path, '/');
  return slash != nullptr ? slash + 1 : path;
}

}  // namespace

void LogAt(LogLevel level, const char* file, int line, const char* fmt, ...) {
  timespec ts{};
  clock_gettime(CLOCK_REALTIME, &ts);

  char stamp[32];
  tm tm_buf{};
  localtime_r(&ts.tv_sec, &tm_buf);
  std::strftime(stamp, sizeof(stamp), "%H:%M:%S", &tm_buf);

  char msg[1024];
  va_list ap;
  va_start(ap, fmt);
  std::vsnprintf(msg, sizeof(msg), fmt, ap);
  va_end(ap);

  const std::string_view name = LevelName(level);
  // One write, no flush per call: stderr is unbuffered on POSIX already.
  std::fprintf(stderr, "%s.%06ld %-5.*s %s:%d] %s\n", stamp, ts.tv_nsec / 1000,
               static_cast<int>(name.size()), name.data(), Basename(file), line, msg);
}

}  // namespace raftkv
