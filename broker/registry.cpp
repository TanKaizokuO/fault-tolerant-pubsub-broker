#include "registry.hpp"

#include <algorithm>

namespace broker {

std::string_view to_string(SubscriberState state) {
    switch (state) {
        case SubscriberState::ALIVE: return "ALIVE";
        case SubscriberState::DEAD: return "DEAD";
    }
    return "UNKNOWN";
}

std::optional<PublishMode> parse_publish_mode(std::string_view text) {
    if (text == "unicast") return PublishMode::UNICAST;
    if (text == "multicast") return PublishMode::MULTICAST;
    if (text == "broadcast") return PublishMode::BROADCAST;
    return std::nullopt;
}

std::string_view to_string(PublishMode mode) {
    switch (mode) {
        case PublishMode::UNICAST: return "unicast";
        case PublishMode::MULTICAST: return "multicast";
        case PublishMode::BROADCAST: return "broadcast";
    }
    return "unknown";
}

UpsertResult Registry::upsert(const std::string& id, const std::string& callback_url,
                              const std::vector<std::string>& topics) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = subscribers_.find(id);
    const bool created = it == subscribers_.end();
    if (created) {
        it = subscribers_.emplace(id, Subscriber{}).first;
        it->second.id = id;
    }
    Subscriber& sub = it->second;
    sub.callback_url = callback_url;
    sub.topics = topics;
    sub.state = SubscriberState::ALIVE;
    sub.failures = 0;
    sub.generation = next_generation_++;
    return UpsertResult{created, sub};
}

bool Registry::remove(const std::string& id) {
    std::lock_guard<std::mutex> lock(mutex_);
    return subscribers_.erase(id) > 0;
}

std::optional<Subscriber> Registry::find(const std::string& id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = subscribers_.find(id);
    if (it == subscribers_.end()) return std::nullopt;
    return it->second;
}

std::vector<Subscriber> Registry::list() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Subscriber> out;
    out.reserve(subscribers_.size());
    for (const auto& [id, sub] : subscribers_) out.push_back(sub);
    return out;
}

std::vector<Recipient> Registry::multicast_recipients(const std::string& topic) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Recipient> out;
    for (const auto& [id, sub] : subscribers_) {
        if (sub.state != SubscriberState::ALIVE) continue;
        const auto& topics = sub.topics;
        if (std::find(topics.begin(), topics.end(), topic) == topics.end()) continue;
        out.push_back(Recipient{sub.id, sub.callback_url, sub.generation});
    }
    return out;
}

std::vector<Recipient> Registry::broadcast_recipients() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Recipient> out;
    for (const auto& [id, sub] : subscribers_) {
        if (sub.state != SubscriberState::ALIVE) continue;
        out.push_back(Recipient{sub.id, sub.callback_url, sub.generation});
    }
    return out;
}

std::optional<UnicastTarget> Registry::unicast_target(const std::string& id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = subscribers_.find(id);
    if (it == subscribers_.end()) return std::nullopt;
    const Subscriber& sub = it->second;
    return UnicastTarget{
        Recipient{sub.id, sub.callback_url, sub.generation},
        sub.state == SubscriberState::ALIVE,
    };
}

std::vector<OutcomeApplied> Registry::record_outcomes(
    const std::vector<DeliveryObservation>& observations) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<OutcomeApplied> out;
    out.reserve(observations.size());
    for (const auto& obs : observations) {
        OutcomeApplied applied;
        auto it = subscribers_.find(obs.recipient.id);
        // Removed, or re-registered since the snapshot (new generation even if the
        // URL is unchanged): the result describes a previous incarnation, so drop it.
        if (it != subscribers_.end() &&
            it->second.generation == obs.recipient.generation &&
            it->second.callback_url == obs.recipient.callback_url) {
            Subscriber& sub = it->second;
            applied.applied = true;
            if (obs.delivered) {
                // Only re-registration (upsert) revives a DEAD subscriber. A late
                // success for an earlier snapshot must not reset state or failures.
                // ALIVE already implies failures == 0, so nothing else changes.
                sub.delivered++;
            } else {
                sub.failures++;
                applied.marked_dead = sub.state != SubscriberState::DEAD;
                sub.state = SubscriberState::DEAD;
            }
        }
        out.push_back(applied);
    }
    return out;
}

}  // namespace broker
