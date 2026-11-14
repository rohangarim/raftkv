#pragma once

// A TCP relay for one directed link between two node processes, so a test can
// cut the link without root, tc or iptables. A node is told to reach its peer
// through the relay's port; while the link is "cut" the relay closes every
// open connection and drops new ones on the floor, which is what a partition
// looks like to gRPC on either end (connections reset, then connect attempts
// that go nowhere).

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

namespace raftkv::testing {

class LinkProxy {
 public:
  explicit LinkProxy(uint16_t target_port) : target_port_(target_port) {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(listen_fd_, 64);
    socklen_t len = sizeof(addr);
    ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    port_ = ntohs(addr.sin_port);
    SetNonBlocking(listen_fd_);
    thread_ = std::thread([this] { Loop(); });
  }

  ~LinkProxy() {
    stop_.store(true);
    thread_.join();
    for (auto& p : pipes_) CloseBoth(p);
    ::close(listen_fd_);
  }

  LinkProxy(const LinkProxy&) = delete;
  LinkProxy& operator=(const LinkProxy&) = delete;

  uint16_t port() const { return port_; }
  void SetCut(bool cut) { cut_.store(cut); }

 private:
  struct Pipe {
    int a = -1;  // the dialer's side
    int b = -1;  // the target's side
    std::string a_to_b;
    std::string b_to_a;
  };

  static void SetNonBlocking(int fd) { ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK); }

  static void CloseBoth(Pipe& p) {
    if (p.a >= 0) ::close(p.a);
    if (p.b >= 0) ::close(p.b);
    p.a = p.b = -1;
  }

  int DialTarget() const {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(target_port_);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
      ::close(fd);
      return -1;
    }
    return fd;
  }

  void Accept() {
    while (true) {
      const int a = ::accept(listen_fd_, nullptr, nullptr);
      if (a < 0) return;
      if (cut_.load()) {
        ::close(a);
        continue;
      }
      const int b = DialTarget();
      if (b < 0) {
        ::close(a);
        continue;
      }
      int one = 1;
      ::setsockopt(a, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
      ::setsockopt(b, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
      SetNonBlocking(a);
      SetNonBlocking(b);
      Pipe p;
      p.a = a;
      p.b = b;
      pipes_.push_back(std::move(p));
    }
  }

  // Reads from `from` into `out_for_peer`; false means close the pipe.
  static bool Pump(int from, std::string* out_for_peer) {
    char buf[16384];
    while (out_for_peer->size() < (1U << 20)) {
      const ssize_t n = ::read(from, buf, sizeof(buf));
      if (n > 0) {
        out_for_peer->append(buf, static_cast<size_t>(n));
      } else if (n == 0) {
        return false;
      } else {
        return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR;
      }
    }
    return true;
  }

  static bool Flush(int to, std::string* pending) {
    while (!pending->empty()) {
      const ssize_t n = ::write(to, pending->data(), pending->size());
      if (n > 0) {
        pending->erase(0, static_cast<size_t>(n));
      } else {
        return n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR);
      }
    }
    return true;
  }

  void Loop() {
    while (!stop_.load()) {
      if (cut_.load()) {
        for (auto& p : pipes_) CloseBoth(p);
        pipes_.clear();
      }
      const size_t polled = pipes_.size();
      std::vector<pollfd> fds;
      fds.push_back({listen_fd_, POLLIN, 0});
      for (const auto& p : pipes_) {
        fds.push_back({p.a, static_cast<short>(POLLIN | (p.b_to_a.empty() ? 0 : POLLOUT)), 0});
        fds.push_back({p.b, static_cast<short>(POLLIN | (p.a_to_b.empty() ? 0 : POLLOUT)), 0});
      }
      ::poll(fds.data(), fds.size(), 20);
      if ((fds[0].revents & POLLIN) != 0) Accept();

      for (size_t i = 0; i < polled; ++i) {
        Pipe& p = pipes_[i];
        const short ra = fds[1 + 2 * i].revents;
        const short rb = fds[2 + 2 * i].revents;
        bool ok = true;
        if ((ra | rb) & (POLLERR | POLLNVAL)) ok = false;
        if (ok && (ra & (POLLIN | POLLHUP)) != 0) ok = Pump(p.a, &p.a_to_b);
        if (ok && (rb & (POLLIN | POLLHUP)) != 0) ok = Pump(p.b, &p.b_to_a);
        if (ok) ok = Flush(p.b, &p.a_to_b) && Flush(p.a, &p.b_to_a);
        if (!ok) CloseBoth(p);
      }
      pipes_.erase(std::remove_if(pipes_.begin(), pipes_.end(), [](const Pipe& p) { return p.a < 0; }),
                   pipes_.end());
    }
  }

  uint16_t target_port_;
  uint16_t port_ = 0;
  int listen_fd_ = -1;
  std::atomic<bool> stop_{false};
  std::atomic<bool> cut_{false};
  std::vector<Pipe> pipes_;
  std::thread thread_;
};

}  // namespace raftkv::testing
