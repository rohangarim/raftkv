// Command-line client, for scripts and for poking at a running cluster.
//
//   raftkv_cli --addrs 127.0.0.1:9001,127.0.0.1:9002 put KEY VALUE
//   raftkv_cli --addrs ...                            get KEY
//   raftkv_cli --addrs ...                            delete KEY
//   raftkv_cli --addrs ...                            cas KEY EXPECTED NEW   (EXPECTED "-" = must
//   not exist)
//
// Exit codes are part of the interface, because a script has to tell a clean
// "no" apart from "I could not find out":
//   0  success (get printed the value to stdout)
//   3  get: key not found
//   4  cas: comparison failed (applied and the answer is no)
//   5  timed out: a WRITE may or may not have been applied
//   1  any other failure

#include <cstdio>
#include <exception>
#include <sstream>
#include <string>
#include <vector>

#include "client/kv_client.h"

namespace {

std::vector<std::string> Split(const std::string& s, char sep) {
  std::vector<std::string> out;
  std::stringstream ss(s);
  std::string part;
  while (std::getline(ss, part, sep)) {
    if (!part.empty()) {
      out.push_back(part);
    }
  }
  return out;
}

int Fail(const raftkv::lsm::Status& s) {
  std::fprintf(stderr, "%s\n", s.ToString().c_str());
  return s.ErrCode() == raftkv::lsm::Code::kTimedOut ? 5 : 1;
}

int Run(int argc, char** argv) {
  raftkv::client::ClientOptions options;
  std::vector<std::string> rest;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--addrs" && i + 1 < argc) {
      options.addresses = Split(argv[++i], ',');
    } else if (arg == "--client-id" && i + 1 < argc) {
      options.client_id = argv[++i];
    } else if (arg == "--timeout-ms" && i + 1 < argc) {
      options.total_timeout = std::chrono::milliseconds(std::atoll(argv[++i]));
    } else {
      rest.push_back(arg);
    }
  }
  if (options.addresses.empty() || rest.empty()) {
    std::fprintf(stderr,
                 "usage: raftkv_cli --addrs a,b,c [--client-id ID] [--timeout-ms N] "
                 "put K V | get K | delete K | cas K EXPECTED NEW\n");
    return 2;
  }

  raftkv::client::KvClient client(options);
  const std::string& op = rest[0];

  if (op == "put" && rest.size() == 3) {
    const auto s = client.Put(rest[1], rest[2]);
    return s.IsOk() ? 0 : Fail(s);
  }
  if (op == "get" && rest.size() == 2) {
    auto got = client.Get(rest[1]);
    if (!got.IsOk()) {
      return Fail(got.GetStatus());
    }
    const std::optional<std::string>& value = *got;
    if (!value.has_value()) {
      return 3;
    }
    std::fwrite(value->data(), 1, value->size(), stdout);
    std::fputc('\n', stdout);
    return 0;
  }
  if (op == "delete" && rest.size() == 2) {
    const auto s = client.Delete(rest[1]);
    return s.IsOk() ? 0 : Fail(s);
  }
  if (op == "cas" && rest.size() == 4) {
    std::optional<std::string> expected;
    if (rest[2] != "-") {
      expected = rest[2];
    }
    auto outcome = client.Cas(rest[1], expected, rest[3]);
    if (!outcome.IsOk()) {
      return Fail(outcome.GetStatus());
    }
    return outcome->applied ? 0 : 4;
  }
  std::fprintf(stderr, "bad command\n");
  return 2;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    return Run(argc, argv);
  } catch (const std::exception& e) {
    std::fprintf(stderr, "fatal: %s\n", e.what());
  } catch (...) {
    std::fprintf(stderr, "fatal: unknown exception\n");
  }
  return 1;
}
