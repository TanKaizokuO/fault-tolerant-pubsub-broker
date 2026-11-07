// C++17 subscriber for the fault-tolerant pub/sub broker (docs/spec.md §5).
//
// Lifecycle: start the callback HTTP server, register with the broker
// (retrying every 1 s for up to 10 s), then receive POST /event callbacks,
// deduplicate them by event_id, and log every significant action. On
// SIGINT/SIGTERM it stops without deregistering: exiting is the crash
// simulation used by the failure demo; explicit deregistration is done with
// `curl -X DELETE`.

#include "httplib.h"
#include "json.hpp"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <csignal>
#include <ctime>
#include <deque>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_set>
#include <vector>

#include <pthread.h>
#include <signal.h>

namespace {

using json = nlohmann::json;

constexpr std::size_t kDedupCapacity = 1000;
constexpr std::chrono::seconds kRegisterTimeout{10};
constexpr std::chrono::seconds kRegisterRetryInterval{1};
constexpr int kConnectTimeoutSec = 1;
constexpr int kIoTimeoutSec = 2;
constexpr const char* kCallbackBindHost = "127.0.0.1";
constexpr const char* kDefaultBroker = "http://localhost:8080";
constexpr const char* kBrokerScheme = "http://";

struct Options {
    std::string id;
    int port = 0;
    std::vector<std::string> topics;
    std::string broker = kDefaultBroker;
};

enum class ParseResult { Ok, Help, Error };

void print_usage(std::ostream& out) {
    out << "Usage: subscriber --id <id> --port <port> [--topics <t1,t2,...>] [--broker <url>]\n"
        << "  --id <id>        subscriber id (required)\n"
        << "  --port <port>    local callback port, 1-65535 (required)\n"
        << "  --topics <list>  comma-separated topics (optional)\n"
        << "  --broker <url>   broker base URL (default " << kDefaultBroker << ")\n"
        << "  -h, --help       show this help\n";
}

std::string trim(const std::string& s) {
    const auto first = s.find_first_not_of(" \t");
    if (first == std::string::npos) {
        return {};
    }
    const auto last = s.find_last_not_of(" \t");
    return s.substr(first, last - first + 1);
}

bool is_valid_id(const std::string& id) {
    return !id.empty();
}

std::optional<int> parse_port(const std::string& s) {
    int value = 0;
    const char* end = s.data() + s.size();
    const auto [ptr, ec] = std::from_chars(s.data(), end, value);
    if (ec != std::errc{} || ptr != end || value < 1 || value > 65535) {
        return std::nullopt;
    }
    return value;
}

bool parse_topics(const std::string& list, std::vector<std::string>& out, std::string& err) {
    out.clear();
    if (trim(list).empty()) {
        return true;
    }
    std::size_t start = 0;
    for (;;) {
        const auto comma = list.find(',', start);
        const auto raw = list.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        const auto topic = trim(raw);
        if (topic.empty()) {
            err = "--topics contains an empty topic name";
            return false;
        }
        out.push_back(topic);
        if (comma == std::string::npos) {
            return true;
        }
        start = comma + 1;
    }
}

// Accepts only http://host[:port], optionally followed by slashes, with no path, query,
// or fragment. Returns the normalized base URL (no trailing slash) in `normalized`.
bool normalize_broker_url(const std::string& url, std::string& normalized, std::string& err) {
    const std::string scheme = kBrokerScheme;
    if (url.rfind(scheme, 0) != 0) {
        err = "--broker must start with http:// (got '" + url + "')";
        return false;
    }
    std::string authority = url.substr(scheme.size());
    while (!authority.empty() && authority.back() == '/') {
        authority.pop_back();
    }
    if (authority.empty() || authority.find_first_of("/ \t?#") != std::string::npos) {
        err = "--broker must be http://host[:port] without a path, query, or fragment (got '" +
              url + "')";
        return false;
    }
    normalized = scheme + authority;
    return true;
}

ParseResult parse_args(int argc, char** argv, Options& opts, std::string& err) {
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") {
            return ParseResult::Help;
        }

        std::string name = arg;
        std::optional<std::string> inline_value;
        const auto eq = arg.find('=');
        if (arg.rfind("--", 0) == 0 && eq != std::string::npos) {
            name = arg.substr(0, eq);
            inline_value = arg.substr(eq + 1);
        }
        if (name != "--id" && name != "--port" && name != "--topics" && name != "--broker") {
            err = "unknown argument '" + arg + "'";
            return ParseResult::Error;
        }

        std::string value;
        if (inline_value) {
            value = *inline_value;
        } else if (i + 1 < argc) {
            value = argv[++i];
        } else {
            err = name + " requires a value";
            return ParseResult::Error;
        }

        if (name == "--id") {
            opts.id = value;
        } else if (name == "--port") {
            const auto port = parse_port(value);
            if (!port) {
                err = "--port must be an integer in 1-65535 (got '" + value + "')";
                return ParseResult::Error;
            }
            opts.port = *port;
        } else if (name == "--topics") {
            if (!parse_topics(value, opts.topics, err)) {
                return ParseResult::Error;
            }
        } else if (!normalize_broker_url(value, opts.broker, err)) {
            return ParseResult::Error;
        }
    }

    if (!is_valid_id(opts.id)) {
        err = "--id is required and must be non-empty";
        return ParseResult::Error;
    }
    if (opts.port == 0) {
        err = "--port is required";
        return ParseResult::Error;
    }
    return ParseResult::Ok;
}

std::string format_topics(const std::vector<std::string>& topics) {
    std::string out = "[";
    for (std::size_t i = 0; i < topics.size(); ++i) {
        if (i > 0) {
            out += ',';
        }
        out += topics[i];
    }
    return out + "]";
}

std::string describe_signal(int sig) {
    switch (sig) {
        case SIGINT:
            return "SIGINT";
        case SIGTERM:
            return "SIGTERM";
        default:
            return "signal " + std::to_string(sig);
    }
}

std::string clock_stamp() {
    using namespace std::chrono;
    const auto now = system_clock::now();
    const auto millis = duration_cast<milliseconds>(now.time_since_epoch()).count() % 1000;
    const std::time_t secs = system_clock::to_time_t(now);
    std::tm tm{};
    localtime_r(&secs, &tm);
    std::ostringstream out;
    out << std::put_time(&tm, "%H:%M:%S") << '.' << std::setw(3) << std::setfill('0') << millis;
    return out.str();
}

void log_line(const std::string& tag, const std::string& message) {
    static std::mutex mutex;
    const std::string stamp = clock_stamp();
    std::lock_guard<std::mutex> lock(mutex);
    std::cout << '[' << stamp << "] [" << tag << "] " << message << std::endl;
}

// Bounded FIFO set of recently seen event_ids. The check-and-insert is a single
// critical section, so two concurrent deliveries of the same event_id cannot
// both be reported as new.
class DedupWindow {
public:
    explicit DedupWindow(std::size_t capacity) : capacity_(capacity) {}

    // Returns true if `id` was not in the window (and records it), false if it was already seen.
    bool insert_if_new(const std::string& id) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!seen_.insert(id).second) {
            return false;
        }
        order_.push_back(id);
        if (order_.size() > capacity_) {
            seen_.erase(order_.front());
            order_.pop_front();
        }
        return true;
    }

private:
    std::mutex mutex_;
    std::size_t capacity_;
    std::unordered_set<std::string> seen_;
    std::deque<std::string> order_;
};

struct Event {
    std::string event_id;
    std::string mode;
    std::optional<std::string> topic;
    json payload;
};

bool require_non_empty_string(const json& doc, const char* key, std::string& out, std::string& err) {
    const auto it = doc.find(key);
    if (it == doc.end() || !it->is_string() || it->get_ref<const std::string&>().empty()) {
        err = std::string("'") + key + "' must be a non-empty string";
        return false;
    }
    out = it->get_ref<const std::string&>();
    return true;
}

bool parse_event(const std::string& body, Event& event, std::string& err) {
    json doc;
    try {
        doc = json::parse(body);
    } catch (const json::parse_error& e) {
        err = std::string("malformed JSON: ") + e.what();
        return false;
    }
    if (!doc.is_object()) {
        err = "body must be a JSON object";
        return false;
    }
    if (!require_non_empty_string(doc, "event_id", event.event_id, err)) {
        return false;
    }
    if (!require_non_empty_string(doc, "mode", event.mode, err)) {
        return false;
    }
    if (event.mode != "unicast" && event.mode != "multicast" && event.mode != "broadcast") {
        err = "'mode' must be unicast, multicast, or broadcast";
        return false;
    }

    const auto topic = doc.find("topic");
    if (topic == doc.end()) {
        err = "'topic' is required (string or null)";
        return false;
    }
    if (topic->is_null()) {
        event.topic.reset();
    } else if (topic->is_string()) {
        event.topic = topic->get<std::string>();
    } else {
        err = "'topic' must be a string or null";
        return false;
    }

    std::string timestamp;
    if (!require_non_empty_string(doc, "timestamp", timestamp, err)) {
        return false;
    }

    const auto payload = doc.find("payload");
    if (payload == doc.end()) {
        err = "'payload' is required";
        return false;
    }
    event.payload = *payload;
    return true;
}

void respond_json(httplib::Response& res, int status, const json& body) {
    res.status = status;
    res.set_content(body.dump(), "application/json");
}

void handle_event(const httplib::Request& req, httplib::Response& res, const std::string& id,
                  DedupWindow& dedup) {
    try {
        Event event;
        std::string err;
        if (!parse_event(req.body, event, err)) {
            log_line(id, "REJECTED malformed event: " + err);
            respond_json(res, 400, json{{"error", err}});
            return;
        }

        if (!dedup.insert_if_new(event.event_id)) {
            log_line(id, "DUPLICATE " + event.event_id + " ignored");
            respond_json(res, 200, json{{"status", "duplicate"}});
            return;
        }

        std::string line = "EVENT " + event.event_id + " " + event.mode;
        if (event.topic) {
            line += " topic=" + *event.topic;
        }
        line += " payload=" + event.payload.dump();
        log_line(id, line);
        respond_json(res, 200, json{{"status", "processed"}});
    } catch (const std::exception& e) {
        respond_json(res, 500, json{{"error", std::string("internal error: ") + e.what()}});
    }
}

struct Attempt {
    bool ok = false;
    std::string detail;
};

std::string broker_error_detail(const std::string& body) {
    try {
        const json doc = json::parse(body);
        if (doc.is_object() && doc.contains("error") && doc["error"].is_string()) {
            return doc["error"].get<std::string>();
        }
    } catch (const json::exception&) {
        // Not JSON: fall through and report the raw body.
    }
    return body.empty() ? std::string("empty response body") : body;
}

// Every failure is retryable: a connection error or any non-2xx response. The
// 10 s window in register_with_retries bounds the total retry time.
Attempt try_register(httplib::Client& client, const Options& opts, const std::string& callback_url) {
    const json body = {{"id", opts.id}, {"callback_url", callback_url}, {"topics", opts.topics}};
    auto res = client.Post("/subscribe", body.dump(), "application/json");
    if (!res) {
        return {false, "connection error: " + httplib::to_string(res.error())};
    }
    if (res->status >= 200 && res->status < 300) {
        return {true, {}};
    }
    return {false, "HTTP " + std::to_string(res->status) + ": " + broker_error_detail(res->body)};
}

// Waits up to `duration` for one of the blocked termination signals.
// Returns the signal number, or 0 on timeout.
int wait_for_signal(const sigset_t& signals, std::chrono::milliseconds duration) {
    timespec ts{};
    ts.tv_sec = static_cast<std::time_t>(duration.count() / 1000);
    ts.tv_nsec = static_cast<long>((duration.count() % 1000) * 1000000);
    siginfo_t info{};
    const int sig = sigtimedwait(&signals, &info, &ts);
    return sig > 0 ? sig : 0;
}

enum class RegisterOutcome { Registered, Failed, Interrupted };

RegisterOutcome register_with_retries(httplib::Client& client, const Options& opts,
                                      const std::string& callback_url, const sigset_t& signals) {
    const auto deadline = std::chrono::steady_clock::now() + kRegisterTimeout;
    for (int attempt = 1;; ++attempt) {
        const Attempt result = try_register(client, opts, callback_url);
        if (result.ok) {
            log_line(opts.id, "REGISTERED with " + opts.broker + " topics=" + format_topics(opts.topics) +
                                  " -> " + callback_url + " (attempt " + std::to_string(attempt) + ")");
            return RegisterOutcome::Registered;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            log_line(opts.id, "REGISTER failed after " + std::to_string(attempt) +
                                  " attempts within 10 s: " + result.detail);
            return RegisterOutcome::Failed;
        }
        const auto remaining = std::max(
            std::chrono::milliseconds::zero(),
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()));
        const auto pause = std::min(std::chrono::duration_cast<std::chrono::milliseconds>(kRegisterRetryInterval),
                                    remaining);
        log_line(opts.id, "REGISTER attempt " + std::to_string(attempt) + " failed: " + result.detail +
                              "; retrying in " + std::to_string(pause.count()) + " ms");
        if (const int sig = wait_for_signal(signals, pause); sig != 0) {
            log_line(opts.id, "received " + describe_signal(sig) + " during registration; aborting");
            return RegisterOutcome::Interrupted;
        }
    }
}

}  // namespace

int main(int argc, char** argv) {
    Options opts;
    std::string err;
    switch (parse_args(argc, argv, opts, err)) {
        case ParseResult::Help:
            print_usage(std::cout);
            return 0;
        case ParseResult::Error:
            std::cerr << "error: " << err << "\n";
            print_usage(std::cerr);
            return 2;
        case ParseResult::Ok:
            break;
    }

    httplib::Client client(opts.broker);
    if (!client.is_valid()) {
        std::cerr << "error: invalid --broker URL '" << opts.broker << "'\n";
        return 2;
    }
    client.set_connection_timeout(kConnectTimeoutSec, 0);
    client.set_read_timeout(kIoTimeoutSec, 0);
    client.set_write_timeout(kIoTimeoutSec, 0);

    // Block termination signals before any thread is created so every thread
    // inherits the mask; they are then consumed by sigtimedwait/sigwait rather
    // than killing the process asynchronously.
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    if (const int rc = pthread_sigmask(SIG_BLOCK, &signals, nullptr); rc != 0) {
        std::cerr << "error: pthread_sigmask failed: " << rc << "\n";
        return 1;
    }
    // A broker closing a connection mid-write must surface as a send error, not kill us.
    std::signal(SIGPIPE, SIG_IGN);

    const std::string callback_url = "http://localhost:" + std::to_string(opts.port) + "/event";

    DedupWindow dedup(kDedupCapacity);
    httplib::Server server;
    server.Post("/event", [&opts, &dedup](const httplib::Request& req, httplib::Response& res) {
        handle_event(req, res, opts.id, dedup);
    });

    if (!server.bind_to_port(kCallbackBindHost, opts.port)) {
        log_line(opts.id, "failed to bind callback server on " + std::string(kCallbackBindHost) + ":" +
                              std::to_string(opts.port) + " (port in use or unavailable)");
        return 1;
    }
    std::thread server_thread([&server] { server.listen_after_bind(); });
    server.wait_until_ready();
    if (!server.is_running()) {
        log_line(opts.id, "callback server failed to listen on " + std::string(kCallbackBindHost) + ":" +
                              std::to_string(opts.port));
        server_thread.join();
        return 1;
    }
    log_line(opts.id, "LISTENING " + callback_url);

    const RegisterOutcome outcome = register_with_retries(client, opts, callback_url, signals);
    int exit_code = 0;
    if (outcome == RegisterOutcome::Registered) {
        int sig = 0;
        if (const int rc = sigwait(&signals, &sig); rc != 0) {
            log_line(opts.id, "sigwait failed: " + std::to_string(rc));
        } else {
            log_line(opts.id, "received " + describe_signal(sig) +
                                  ", shutting down without deregistering");
        }
    } else {
        exit_code = 1;
    }

    server.stop();
    server_thread.join();
    return exit_code;
}
