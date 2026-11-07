// Broker: REST front end for the registry, publish fan-out, and delivery reports.
// Usage: broker [--port N]   (default 8080)

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <future>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include "delivery.hpp"
#include "httplib.h"
#include "json.hpp"
#include "log.hpp"
#include "registry.hpp"

namespace {

using broker::DeliveryRequest;
using broker::DeliveryResult;
using broker::DeliveryObservation;
using broker::PublishMode;
using broker::Recipient;
using broker::Registry;
using json = nlohmann::json;

constexpr int kDefaultPort = 8080;

json error_body(const std::string& message) { return json{{"error", message}}; }

void send_json(httplib::Response& res, int status, const json& body) {
    res.status = status;
    res.set_content(body.dump(), "application/json");
}

// Parses a request body that must be a JSON object. nullopt otherwise.
std::optional<json> parse_object(const std::string& body) {
    const json parsed = json::parse(body, nullptr, false);
    if (parsed.is_discarded() || !parsed.is_object()) return std::nullopt;
    return parsed;
}

// Returns the string value of `key`, or nullopt if absent, not a string, or empty.
std::optional<std::string> required_string(const json& obj, const char* key) {
    auto it = obj.find(key);
    if (it == obj.end() || !it->is_string()) return std::nullopt;
    std::string value = it->get<std::string>();
    if (value.empty()) return std::nullopt;
    return value;
}

std::string join_list(const std::vector<std::string>& items) {
    std::string out = "[";
    for (std::size_t i = 0; i < items.size(); ++i) {
        if (i) out += ",";
        out += items[i];
    }
    return out + "]";
}

std::string join_ids(const std::vector<Recipient>& recipients) {
    std::vector<std::string> ids;
    ids.reserve(recipients.size());
    for (const auto& r : recipients) ids.push_back(r.id);
    return join_list(ids);
}

json subscriber_json(const broker::Subscriber& sub) {
    return json{
        {"id", sub.id},
        {"callback_url", sub.callback_url},
        {"topics", sub.topics},
        {"state", std::string(broker::to_string(sub.state))},
        {"failures", sub.failures},
        {"delivered", sub.delivered},
    };
}

// ---- POST /subscribe ------------------------------------------------------

void handle_subscribe(Registry& registry, const httplib::Request& req, httplib::Response& res) {
    const auto body = parse_object(req.body);
    if (!body) return send_json(res, 400, error_body("body must be a JSON object"));

    const auto id = required_string(*body, "id");
    if (!id || id->find('/') != std::string::npos) {
        return send_json(res, 400, error_body("'id' must be a non-empty string without '/'"));
    }

    auto url_it = body->find("callback_url");
    if (url_it == body->end() || !url_it->is_string() ||
        !broker::parse_callback_url(url_it->get<std::string>())) {
        return send_json(res, 400, error_body("'callback_url' must be an http:// URL"));
    }
    const std::string callback_url = url_it->get<std::string>();

    auto topics_it = body->find("topics");
    if (topics_it == body->end() || !topics_it->is_array()) {
        return send_json(res, 400, error_body("'topics' must be an array of strings"));
    }
    std::vector<std::string> topics;
    for (const auto& topic : *topics_it) {
        if (!topic.is_string()) {
            return send_json(res, 400, error_body("'topics' must be an array of strings"));
        }
        topics.push_back(topic.get<std::string>());
    }

    const auto result = registry.upsert(*id, callback_url, topics);
    broker::log_line("broker", "REGISTER " + *id + " topics=" + join_list(topics) + " -> " +
                                   callback_url);

    const json reply{
        {"id", result.subscriber.id},
        {"state", std::string(broker::to_string(result.subscriber.state))},
        {"topics", result.subscriber.topics},
    };
    send_json(res, result.created ? 201 : 200, reply);
}

// ---- DELETE /subscribe/{id} -----------------------------------------------

void handle_unsubscribe(Registry& registry, const httplib::Request& req, httplib::Response& res) {
    const std::string id = req.matches[1];
    if (!registry.remove(id)) {
        return send_json(res, 404, error_body("unknown subscriber: " + id));
    }
    broker::log_line("broker", "DEREGISTER " + id);
    send_json(res, 200, json{{"id", id}, {"removed", true}});
}

// ---- POST /publish --------------------------------------------------------

// Builds the per-recipient delivery report entry for a finished delivery.
json delivery_entry(const Recipient& recipient, const DeliveryResult& result) {
    json entry{{"id", recipient.id}, {"attempts", result.attempts}};
    if (result.delivered) {
        entry["status"] = "DELIVERED";
        entry["ack"] = result.ack.empty() ? json(nullptr) : json(result.ack);
    } else {
        entry["status"] = "FAILED";
        entry["error"] = result.error;
    }
    return entry;
}

void handle_publish(Registry& registry, std::atomic<std::uint64_t>& event_counter,
                    const httplib::Request& req, httplib::Response& res) {
    const auto body = parse_object(req.body);
    if (!body) return send_json(res, 400, error_body("body must be a JSON object"));

    const auto mode_text = required_string(*body, "mode");
    const auto mode = mode_text ? broker::parse_publish_mode(*mode_text) : std::nullopt;
    if (!mode) {
        return send_json(res, 400, error_body("'mode' must be unicast, multicast, or broadcast"));
    }

    std::string target;
    if (*mode != PublishMode::BROADCAST) {
        const auto target_text = required_string(*body, "target");
        if (!target_text) {
            return send_json(res, 400, error_body("'target' is required for unicast and multicast"));
        }
        target = *target_text;
    }

    std::optional<std::string> supplied_event_id;
    if (body->contains("event_id")) {
        supplied_event_id = required_string(*body, "event_id");
        if (!supplied_event_id) {
            return send_json(res, 400, error_body("'event_id' must be a non-empty string"));
        }
    }
    const json payload = body->contains("payload") ? (*body)["payload"] : json(nullptr);

    // Recipient selection. Unicast reports dead subscribers as SKIPPED_DEAD.
    std::vector<Recipient> recipients;
    std::vector<Recipient> skipped_dead;
    switch (*mode) {
        case PublishMode::UNICAST: {
            const auto unicast = registry.unicast_target(target);
            if (!unicast) return send_json(res, 404, error_body("unknown subscriber: " + target));
            if (unicast->alive) {
                recipients.push_back(unicast->recipient);
            } else {
                skipped_dead.push_back(unicast->recipient);
            }
            break;
        }
        case PublishMode::MULTICAST:
            recipients = registry.multicast_recipients(target);
            break;
        case PublishMode::BROADCAST:
            recipients = registry.broadcast_recipients();
            break;
    }

    const std::string event_id = supplied_event_id
                                     ? *supplied_event_id
                                     : "e-" + std::to_string(event_counter.fetch_add(1) + 1);
    const std::string mode_name(broker::to_string(*mode));

    std::string publish_line = "PUBLISH " + event_id + " " + mode_name;
    if (*mode != PublishMode::BROADCAST) publish_line += " target=" + target;
    publish_line += " recipients=" + join_ids(recipients);
    broker::log_line("broker", publish_line);
    for (const auto& dead : skipped_dead) {
        broker::log_line("broker", "SKIP " + event_id + " -> " + dead.id + " (DEAD)");
    }

    json deliveries = json::array();
    for (const auto& dead : skipped_dead) {
        deliveries.push_back(json{{"id", dead.id}, {"status", "SKIPPED_DEAD"}, {"attempts", 0}});
    }

    if (!recipients.empty()) {
        const json event{
            {"event_id", event_id},
            {"mode", mode_name},
            {"topic", *mode == PublishMode::MULTICAST ? json(target) : json(nullptr)},
            {"payload", payload},
            {"timestamp", broker::utc_timestamp_iso8601()},
        };
        const std::string event_body = event.dump();

        // Fan-out: one thread per recipient. The registry mutex is not held here.
        std::vector<std::future<DeliveryResult>> futures;
        futures.reserve(recipients.size());
        for (const auto& recipient : recipients) {
            DeliveryRequest request{event_id, recipient.id, recipient.callback_url, event_body};
            futures.push_back(std::async(std::launch::async, [request = std::move(request)] {
                return broker::deliver(request);
            }));
        }

        // Join all, then apply every outcome in one registry update.
        std::vector<DeliveryResult> results;
        std::vector<DeliveryObservation> observations;
        results.reserve(recipients.size());
        observations.reserve(recipients.size());
        for (std::size_t i = 0; i < recipients.size(); ++i) {
            results.push_back(futures[i].get());
            observations.push_back(DeliveryObservation{recipients[i], results.back().delivered});
        }
        const auto applied = registry.record_outcomes(observations);

        for (std::size_t i = 0; i < recipients.size(); ++i) {
            const auto& recipient = recipients[i];
            if (!applied[i].applied) {
                broker::log_line("broker", "STALE " + event_id + " result for " + recipient.id +
                                               " not applied (removed or re-registered)");
            }
            if (applied[i].marked_dead) {
                broker::log_line("broker", "MARK DEAD " + recipient.id + " after " +
                                               std::to_string(results[i].attempts) + " attempts");
            }
            deliveries.push_back(delivery_entry(recipient, results[i]));
        }
    }

    const json reply{
        {"event_id", event_id},
        {"mode", mode_name},
        {"target", *mode == PublishMode::BROADCAST ? json(nullptr) : json(target)},
        {"deliveries", deliveries},
    };
    send_json(res, 200, reply);
}

// ---- GET /subscribers, GET /health ----------------------------------------

void handle_list(Registry& registry, httplib::Response& res) {
    json out = json::array();
    for (const auto& sub : registry.list()) out.push_back(subscriber_json(sub));
    send_json(res, 200, out);
}

// ---- CLI ------------------------------------------------------------------

std::optional<int> parse_port(int argc, char** argv) {
    int port = kDefaultPort;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            const long value = std::strtol(argv[++i], nullptr, 10);
            if (value < 1 || value > 65535) return std::nullopt;
            port = static_cast<int>(value);
        } else {
            return std::nullopt;
        }
    }
    return port;
}

}  // namespace

int main(int argc, char** argv) {
    const auto port = parse_port(argc, argv);
    if (!port) {
        std::cerr << "usage: broker [--port N]\n";
        return 2;
    }

    Registry registry;
    std::atomic<std::uint64_t> event_counter{0};
    httplib::Server server;

    server.Get("/health", [](const httplib::Request&, httplib::Response& res) {
        send_json(res, 200, json{{"status", "ok"}});
    });
    server.Post("/subscribe", [&](const httplib::Request& req, httplib::Response& res) {
        handle_subscribe(registry, req, res);
    });
    server.Delete(R"(/subscribe/([^/]+))", [&](const httplib::Request& req, httplib::Response& res) {
        handle_unsubscribe(registry, req, res);
    });
    server.Post("/publish", [&](const httplib::Request& req, httplib::Response& res) {
        handle_publish(registry, event_counter, req, res);
    });
    server.Get("/subscribers", [&](const httplib::Request&, httplib::Response& res) {
        handle_list(registry, res);
    });

    // Unknown routes and wrong methods still answer with the {"error": ...} shape.
    server.set_error_handler([](const httplib::Request&, httplib::Response& res) {
        if (res.body.empty()) {
            send_json(res, res.status, error_body("request failed with status " +
                                                  std::to_string(res.status)));
        }
    });
    server.set_exception_handler([](const httplib::Request&, httplib::Response& res,
                                    std::exception_ptr ep) {
        std::string message = "internal error";
        try {
            if (ep) std::rethrow_exception(ep);
        } catch (const std::exception& e) {
            message = e.what();
        } catch (...) {
        }
        send_json(res, 500, error_body(message));
    });

    broker::log_line("broker", "listening on 127.0.0.1:" + std::to_string(*port));
    if (!server.listen("127.0.0.1", *port)) {
        broker::log_line("broker", "failed to bind 127.0.0.1:" + std::to_string(*port));
        return 1;
    }
    return 0;
}
