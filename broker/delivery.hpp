// HTTP callback delivery with per-attempt timeouts and bounded retries.
#pragma once

#include <optional>
#include <string>

namespace broker {

// A callback URL split into the pieces httplib needs.
struct CallbackTarget {
    std::string host;
    int port = 80;
    std::string path;
};

// Accepts "http://host[:port][/path]" only. nullopt when malformed.
std::optional<CallbackTarget> parse_callback_url(const std::string& url);

struct DeliveryRequest {
    std::string event_id;      // for logging
    std::string subscriber_id; // for logging
    std::string callback_url;
    std::string body;          // serialized event JSON
};

struct DeliveryResult {
    bool delivered = false;
    int attempts = 0;
    std::string ack;   // subscriber's response status ("processed"/"duplicate"); empty if absent
    std::string error; // reason for the last failed attempt; empty on success
};

// Blocking. Up to 3 attempts; backoff 200 ms then 400 ms between attempts.
// Retries connection errors, timeouts, and HTTP 5xx. HTTP 4xx and other
// non-2xx statuses fail after one attempt.
DeliveryResult deliver(const DeliveryRequest& request);

}  // namespace broker
