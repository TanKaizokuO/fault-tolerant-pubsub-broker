// Logging helpers shared by the broker. Header-only.
#pragma once

#include <chrono>
#include <cstdio>
#include <ctime>
#include <mutex>
#include <string>

namespace broker {

// One line per action: "[HH:MM:SS.mmm] [tag] message".
inline void log_line(const std::string& tag, const std::string& message) {
    static std::mutex mutex;
    using namespace std::chrono;
    const auto now = system_clock::now();
    const auto secs = system_clock::to_time_t(now);
    const auto millis =
        duration_cast<milliseconds>(now.time_since_epoch()).count() % 1000;
    std::tm local{};
    localtime_r(&secs, &local);
    char stamp[16];
    std::snprintf(stamp, sizeof(stamp), "%02d:%02d:%02d.%03d", local.tm_hour,
                  local.tm_min, local.tm_sec, static_cast<int>(millis));
    std::lock_guard<std::mutex> lock(mutex);
    std::printf("[%s] [%s] %s\n", stamp, tag.c_str(), message.c_str());
    std::fflush(stdout);
}

// Broker timestamp in ISO 8601 UTC, e.g. "2026-11-07T10:15:30Z".
inline std::string utc_timestamp_iso8601() {
    const auto secs = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::tm utc{};
    gmtime_r(&secs, &utc);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &utc);
    return buf;
}

}  // namespace broker
