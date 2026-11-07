// Subscriber registry and routing. Pure logic: no sockets, no HTTP, no logging.
// Thread-safe: every public method takes the internal mutex.
#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace broker {

enum class SubscriberState { ALIVE, DEAD };

std::string_view to_string(SubscriberState state);

struct Subscriber {
    std::string id;
    std::string callback_url;
    std::vector<std::string> topics;
    SubscriberState state = SubscriberState::ALIVE;
    std::uint64_t failures = 0;
    std::uint64_t delivered = 0;
    // Bumped on every registration of this id. Lets in-flight delivery results
    // detect that the subscriber was re-registered or removed meanwhile.
    std::uint64_t generation = 0;
};

// Snapshot of what a delivery needs, captured under the registry mutex.
struct Recipient {
    std::string id;
    std::string callback_url;
    std::uint64_t generation = 0;
};

enum class PublishMode { UNICAST, MULTICAST, BROADCAST };

std::optional<PublishMode> parse_publish_mode(std::string_view text);
std::string_view to_string(PublishMode mode);

// Result of one delivery series (all attempts for one recipient).
struct DeliveryObservation {
    Recipient recipient;
    bool delivered = false;
};

// What record_outcomes did for one observation.
struct OutcomeApplied {
    bool applied = false;     // false: subscriber gone or re-registered since snapshot
    bool marked_dead = false; // true: this observation moved the subscriber to DEAD
};

struct UpsertResult {
    bool created = false; // true → 201, false → 200
    Subscriber subscriber;
};

struct UnicastTarget {
    Recipient recipient;
    bool alive = false; // false → report SKIPPED_DEAD, do not attempt
};

class Registry {
public:
    // New id → created; existing id (any state) → replaced, reset to ALIVE, failures = 0.
    UpsertResult upsert(const std::string& id, const std::string& callback_url,
                        const std::vector<std::string>& topics);

    // Returns false when the id is unknown.
    bool remove(const std::string& id);

    std::optional<Subscriber> find(const std::string& id) const;

    // All subscribers, ordered by id.
    std::vector<Subscriber> list() const;

    // ALIVE subscribers whose topics contain `topic`.
    std::vector<Recipient> multicast_recipients(const std::string& topic) const;

    // Every ALIVE subscriber.
    std::vector<Recipient> broadcast_recipients() const;

    // nullopt when the id is unknown.
    std::optional<UnicastTarget> unicast_target(const std::string& id) const;

    // Applies delivery results under one lock. A result is applied only if the
    // subscriber still exists with the same generation as the snapshot.
    //   success → delivered++; state and failures untouched (a DEAD subscriber
    //             stays DEAD until re-registered, even if an earlier success lands late)
    //   failure → failures++, state DEAD
    std::vector<OutcomeApplied> record_outcomes(const std::vector<DeliveryObservation>& observations);

private:
    mutable std::mutex mutex_;
    std::map<std::string, Subscriber> subscribers_;
    std::uint64_t next_generation_ = 1;
};

}  // namespace broker
