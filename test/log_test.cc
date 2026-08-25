#include "common/log.h"

#include <gtest/gtest.h>

namespace raftkv {
namespace {

class LogLevelTest : public ::testing::Test {
 protected:
  void SetUp() override { saved_ = GetLogLevel(); }
  void TearDown() override { SetLogLevel(saved_); }

 private:
  LogLevel saved_ = LogLevel::kInfo;
};

TEST_F(LogLevelTest, EnabledFollowsThreshold) {
  SetLogLevel(LogLevel::kWarn);
  EXPECT_FALSE(LogEnabled(LogLevel::kTrace));
  EXPECT_FALSE(LogEnabled(LogLevel::kDebug));
  EXPECT_FALSE(LogEnabled(LogLevel::kInfo));
  EXPECT_TRUE(LogEnabled(LogLevel::kWarn));
  EXPECT_TRUE(LogEnabled(LogLevel::kError));
}

TEST_F(LogLevelTest, OffSuppressesEverything) {
  SetLogLevel(LogLevel::kOff);
  EXPECT_FALSE(LogEnabled(LogLevel::kError));
}

// The guarantee that matters on the hot path: a disabled log statement must
// not evaluate its arguments.
TEST_F(LogLevelTest, DisabledLevelDoesNotEvaluateArguments) {
  SetLogLevel(LogLevel::kError);
  int calls = 0;
  auto expensive = [&calls] {
    ++calls;
    return 42;
  };
  LOG_DEBUG("value=%d", expensive());
  EXPECT_EQ(calls, 0);

  LOG_ERROR("value=%d", expensive());
  EXPECT_EQ(calls, 1);
}

}  // namespace
}  // namespace raftkv
