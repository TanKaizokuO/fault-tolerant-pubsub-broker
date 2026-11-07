#include "delivery.hpp"

#include <chrono>
#include <cstdlib>
#include <string_view>
#include <thread>

#include "httplib.h"
#include "json.hpp"
#include "log.hpp"

namespace broker {
namespace {

constexpr int kConnectTimeoutSec = 1;
constexpr int kIoTimeoutSec = 2;
constexpr int kMaxAttempts = 3;
constexpr std::chrono::milliseconds kBackoff[] = {
    std::chrono::milliseconds(200),
    std::chrono::milliseconds(400),
};

struct AttemptOutcome {
    bool success = false;
    bool retryable = false;
    std::string ack;
    std::string error;
};

std::string ack_from_body(const std::string& body) {
    const auto parsed = nlohmann::json::parse(body, nullptr, false);
    if (!parsed.is_discarded() && parsed.is_object() && parsed.contains("status") &&
        parsed["status"].is_string()) {
        return parsed["status"].get<std::string>();
    }
    return {};
}

AttemptOutcome attempt_once(const CallbackTarget& target, const std::string& body) {
    httplib::Client client(target.host, target.port);
    client.set_connection_timeout(kConnectTimeoutSec, 0);
    client.set_read_timeout(kIoTimeoutSec, 0);
    client.set_write_timeout(kIoTimeoutSec, 0);

    auto res = client.Post(target.path, body, "application/json");
    if (!res) {
        // Connection failure, timeout, or I/O error: always retryable.
        return AttemptOutcome{false, true, {}, httplib::to_string(res.error())};
    }
    const int status = res->status;
    if (status >= 200 && status < 300) {
        return AttemptOutcome{true, false, ack_from_body(res->body), {}};
    }
    if (status >= 500) {
        return AttemptOutcome{false, true, {}, "HTTP " + std::to_string(status)};
    }
    return AttemptOutcome{false, false, {}, "HTTP " + std::to_string(status)};
}

}  // namespace

std::optional<CallbackTarget> parse_callback_url(const std::string& url) {
    constexpr std::string_view kScheme = "http://";
    if (url.compare(0, kScheme.size(), kScheme) != 0) return std::nullopt;

    const std::string rest = url.substr(kScheme.size());
    const std::size_t slash = rest.find('/');
    const std::string authority = rest.substr(0, slash);
    std::string path = slash == std::string::npos ? "/" : rest.substr(slash);
    // A fragment is client-side only and never part of the request target.
    if (const std::size_t hash = path.find('#'); hash != std::string::npos) path.erase(hash);

    CallbackTarget target;
    target.path = path;

    const std::size_t colon = authority.rfind(':');
    if (colon == std::string::npos) {
        target.host = authority;
    } else {
        target.host = authority.substr(0, colon);
        const std::string port_text = authority.substr(colon + 1);
        if (port_text.empty() || port_text.size() > 5 ||
            port_text.find_first_not_of("0123456789") != std::string::npos) {
            return std::nullopt;
        }
        const long port = std::strtol(port_text.c_str(), nullptr, 10);
        if (port < 1 || port > 65535) return std::nullopt;
        target.port = static_cast<int>(port);
    }

    // A stray ':' survives the port split (e.g. "http://a:b:80/"), and IPv6 literals
    // are unsupported, so any ':' left in the host is malformed. A '?' or '#' here means
    // a query or fragment was written before the path, which is also malformed.
    if (target.host.empty() || target.host.find_first_of("@[]:\\ \t?#") != std::string::npos) {
        return std::nullopt;
    }
    return target;
}

DeliveryResult deliver(const DeliveryRequest& request) {
    DeliveryResult result;
    const auto target = parse_callback_url(request.callback_url);
    if (!target) {
        result.error = "invalid callback URL";
        return result;
    }

    for (int attempt = 1; attempt <= kMaxAttempts; ++attempt) {
        result.attempts = attempt;
        const AttemptOutcome outcome = attempt_once(*target, request.body);
        if (outcome.success) {
            result.delivered = true;
            result.ack = outcome.ack;
            result.error.clear();
            return result;
        }

        result.error = outcome.error;
        log_line("broker", "DELIVER " + request.event_id + " -> " + request.subscriber_id +
                               " attempt " + std::to_string(attempt) + " failed: " + outcome.error);
        if (!outcome.retryable || attempt == kMaxAttempts) break;
        std::this_thread::sleep_for(kBackoff[attempt - 1]);
    }
    return result;
}

}  // namespace broker
