// Unit tests for broker/registry.{hpp,cpp}. Plain checks, no framework; run via ctest.
// CHECK is used instead of assert so the checks also run in NDEBUG (Release) builds.

#include <atomic>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "../broker/registry.hpp"

using namespace broker;

namespace {

int g_failures = 0;

#define CHECK(cond)                                                             \
    do {                                                                        \
        if (!(cond)) {                                                          \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, \
                         #cond);                                                \
            ++g_failures;                                                       \
        }                                                                       \
    } while (0)

const std::string kUrl1 = "http://localhost:9001/event";
const std::string kUrl2 = "http://localhost:9999/event";

// Current snapshot of a registered subscriber, as the publish path would take it.
Recipient snapshot(const Registry& registry, const std::string& id) {
    const auto target = registry.unicast_target(id);
    return target ? target->recipient : Recipient{};
}

// Applies one delivery result for the subscriber's current snapshot.
OutcomeApplied observe(Registry& registry, const std::string& id, bool delivered) {
    return registry.record_outcomes({DeliveryObservation{snapshot(registry, id), delivered}})[0];
}

std::vector<std::string> ids_of(const std::vector<Recipient>& recipients) {
    std::vector<std::string> ids;
    for (const auto& r : recipients) ids.push_back(r.id);
    return ids;
}

void test_new_registration_is_alive() {
    Registry registry;
    const auto result = registry.upsert("S1", kUrl1, {"sports"});
    CHECK(result.created);
    CHECK(result.subscriber.id == "S1");
    CHECK(result.subscriber.callback_url == kUrl1);
    CHECK(result.subscriber.topics == std::vector<std::string>{"sports"});
    CHECK(result.subscriber.state == SubscriberState::ALIVE);
    CHECK(result.subscriber.failures == 0);
}

void test_empty_topics_allowed() {
    Registry registry;
    const auto result = registry.upsert("S1", kUrl1, {});
    CHECK(result.created);
    CHECK(result.subscriber.topics.empty());
}

void test_reregister_replaces_url_topics_and_resets_state() {
    Registry registry;
    registry.upsert("S1", kUrl1, {"sports"});
    observe(registry, "S1", false);  // DEAD, failures = 1

    const auto result = registry.upsert("S1", kUrl2, {"news"});
    CHECK(!result.created);
    CHECK(result.subscriber.callback_url == kUrl2);
    CHECK(result.subscriber.topics == std::vector<std::string>{"news"});
    CHECK(result.subscriber.state == SubscriberState::ALIVE);
    CHECK(result.subscriber.failures == 0);

    const auto stored = registry.find("S1");
    CHECK(stored.has_value());
    CHECK(stored->callback_url == kUrl2);
    CHECK(stored->state == SubscriberState::ALIVE);
}

void test_reregister_same_url_revives_dead_subscriber() {
    Registry registry;
    registry.upsert("S1", kUrl1, {"sports"});
    observe(registry, "S1", false);
    CHECK(registry.find("S1")->state == SubscriberState::DEAD);

    const auto result = registry.upsert("S1", kUrl1, {"sports"});
    CHECK(!result.created);
    CHECK(result.subscriber.state == SubscriberState::ALIVE);
    CHECK(result.subscriber.failures == 0);
}

void test_reregister_changes_generation() {
    Registry registry;
    registry.upsert("S1", kUrl1, {"sports"});
    const auto before = snapshot(registry, "S1").generation;
    registry.upsert("S1", kUrl1, {"sports"});
    const auto after = snapshot(registry, "S1").generation;
    CHECK(before != after);
}

void test_remove() {
    Registry registry;
    registry.upsert("S1", kUrl1, {"sports"});
    CHECK(registry.remove("S1"));
    CHECK(!registry.remove("S1"));
    CHECK(!registry.find("S1").has_value());
    CHECK(!registry.remove("never-registered"));
}

void test_find_and_list_ordering() {
    Registry registry;
    CHECK(!registry.find("S1").has_value());
    registry.upsert("S2", kUrl1, {});
    registry.upsert("S1", kUrl1, {});
    const auto all = registry.list();
    CHECK(all.size() == 2);
    CHECK(all.size() == 2 && all[0].id == "S1");
    CHECK(all.size() == 2 && all[1].id == "S2");
}

void test_unicast_target_unknown_id() {
    Registry registry;
    CHECK(!registry.unicast_target("nobody").has_value());
}

void test_unicast_target_alive_and_dead() {
    Registry registry;
    registry.upsert("S1", kUrl1, {});
    const auto alive = registry.unicast_target("S1");
    CHECK(alive.has_value());
    CHECK(alive && alive->alive);
    CHECK(alive && alive->recipient.id == "S1");
    CHECK(alive && alive->recipient.callback_url == kUrl1);

    observe(registry, "S1", false);
    const auto dead = registry.unicast_target("S1");
    CHECK(dead.has_value());
    CHECK(dead && !dead->alive);
}

void test_multicast_selects_alive_subscribers_with_topic() {
    Registry registry;
    registry.upsert("S1", kUrl1, {"sports"});
    registry.upsert("S2", kUrl1, {"news"});
    registry.upsert("S3", kUrl1, {"sports", "news"});
    registry.upsert("S4", kUrl1, {"sports"});
    observe(registry, "S4", false);  // DEAD: must be excluded

    CHECK((ids_of(registry.multicast_recipients("sports")) ==
           std::vector<std::string>{"S1", "S3"}));
    CHECK((ids_of(registry.multicast_recipients("news")) ==
           std::vector<std::string>{"S2", "S3"}));
    CHECK(registry.multicast_recipients("weather").empty());
}

void test_broadcast_selects_all_alive() {
    Registry registry;
    registry.upsert("S1", kUrl1, {"sports"});
    registry.upsert("S2", kUrl1, {"news"});
    registry.upsert("S3", kUrl1, {});
    observe(registry, "S2", false);  // DEAD: excluded

    CHECK((ids_of(registry.broadcast_recipients()) ==
           std::vector<std::string>{"S1", "S3"}));
}

void test_zero_recipients() {
    Registry registry;
    CHECK(registry.broadcast_recipients().empty());
    CHECK(registry.multicast_recipients("sports").empty());

    registry.upsert("S1", kUrl1, {"sports"});
    observe(registry, "S1", false);
    CHECK(registry.broadcast_recipients().empty());
    CHECK(registry.multicast_recipients("sports").empty());
}

void test_parse_publish_mode() {
    CHECK(parse_publish_mode("unicast") == PublishMode::UNICAST);
    CHECK(parse_publish_mode("multicast") == PublishMode::MULTICAST);
    CHECK(parse_publish_mode("broadcast") == PublishMode::BROADCAST);
    CHECK(!parse_publish_mode("Unicast").has_value());
    CHECK(!parse_publish_mode("").has_value());
    CHECK(!parse_publish_mode("anycast").has_value());
}

void test_success_on_alive_counts_delivery_only() {
    Registry registry;
    registry.upsert("S1", kUrl1, {});
    const auto applied = observe(registry, "S1", true);
    CHECK(applied.applied);
    CHECK(!applied.marked_dead);
    const auto stored = registry.find("S1");
    CHECK(stored && stored->state == SubscriberState::ALIVE);
    CHECK(stored && stored->failures == 0);
    CHECK(stored && stored->delivered == 1);
}

// A late success for a snapshot taken before the subscriber went DEAD must not
// revive it: only re-registration revives a DEAD subscriber.
void test_late_success_does_not_revive_dead() {
    Registry registry;
    registry.upsert("S1", kUrl1, {});
    const auto early = snapshot(registry, "S1");
    observe(registry, "S1", false);  // DEAD, failures = 1

    const auto applied = registry.record_outcomes({DeliveryObservation{early, true}})[0];
    CHECK(applied.applied);
    CHECK(!applied.marked_dead);
    const auto stored = registry.find("S1");
    CHECK(stored && stored->state == SubscriberState::DEAD);
    CHECK(stored && stored->failures == 1);
    CHECK(stored && stored->delivered == 1);
}

// Out-of-order completion: success for an earlier snapshot lands after a later
// failure. The subscriber must stay DEAD with failures intact.
void test_earlier_success_after_later_failure_keeps_dead() {
    Registry registry;
    registry.upsert("S1", kUrl1, {});
    const auto earlier = snapshot(registry, "S1");
    const auto later = snapshot(registry, "S1");
    registry.record_outcomes({DeliveryObservation{later, false}});  // failure completes first: DEAD
    registry.record_outcomes({DeliveryObservation{earlier, true}});

    const auto stored = registry.find("S1");
    CHECK(stored && stored->state == SubscriberState::DEAD);
    CHECK(stored && stored->failures == 1);
}

// Many successes racing one failure, all from the same snapshot. Whatever the
// order, the failure is the last state transition and nothing revives the subscriber.
void test_late_successes_never_revive_dead_under_concurrency() {
    Registry registry;
    registry.upsert("S1", kUrl1, {"sports"});
    const auto snap = snapshot(registry, "S1");

    std::atomic<bool> go{false};
    std::vector<std::thread> threads;
    for (int t = 0; t < 3; ++t) {
        threads.emplace_back([&registry, &snap, &go] {
            while (!go.load()) std::this_thread::yield();
            for (int i = 0; i < 2000; ++i) {
                registry.record_outcomes({DeliveryObservation{snap, true}});
            }
        });
    }
    threads.emplace_back([&registry, &snap, &go] {
        while (!go.load()) std::this_thread::yield();
        registry.record_outcomes({DeliveryObservation{snap, false}});
    });
    go.store(true);
    for (auto& th : threads) th.join();

    // Successes before the failure leave ALIVE/failures=0; the failure then sets
    // DEAD/failures=1; successes after it change nothing. Final state is fixed.
    const auto stored = registry.find("S1");
    CHECK(stored && stored->state == SubscriberState::DEAD);
    CHECK(stored && stored->failures == 1);
    CHECK(stored && stored->delivered <= 3 * 2000);
}


void test_failure_marks_dead_and_counts_failures() {
    Registry registry;
    registry.upsert("S1", kUrl1, {});
    const auto first = observe(registry, "S1", false);
    CHECK(first.applied);
    CHECK(first.marked_dead);
    CHECK(registry.find("S1")->failures == 1);
    CHECK(registry.find("S1")->state == SubscriberState::DEAD);

    const auto second = observe(registry, "S1", false);
    CHECK(second.applied);
    CHECK(!second.marked_dead);  // already DEAD: not a new transition
    CHECK(registry.find("S1")->failures == 2);
}

void test_result_for_removed_subscriber_not_applied() {
    Registry registry;
    registry.upsert("S1", kUrl1, {});
    const auto snap = snapshot(registry, "S1");
    registry.remove("S1");

    const auto applied =
        registry.record_outcomes({DeliveryObservation{snap, false}})[0];
    CHECK(!applied.applied);
    CHECK(!applied.marked_dead);
    CHECK(!registry.find("S1").has_value());
}

void test_stale_failure_after_reregister_same_url_not_applied() {
    Registry registry;
    registry.upsert("S1", kUrl1, {"sports"});
    const auto stale = snapshot(registry, "S1");  // in-flight delivery snapshot
    registry.upsert("S1", kUrl1, {"sports"});     // re-register, URL unchanged

    const auto applied = registry.record_outcomes({DeliveryObservation{stale, false}})[0];
    CHECK(!applied.applied);
    CHECK(!applied.marked_dead);
    const auto stored = registry.find("S1");
    CHECK(stored && stored->state == SubscriberState::ALIVE);
    CHECK(stored && stored->failures == 0);
}

void test_stale_failure_after_revival_does_not_kill_new_registration() {
    Registry registry;
    registry.upsert("S1", kUrl1, {});
    const auto stale = snapshot(registry, "S1");
    observe(registry, "S1", false);  // a concurrent delivery marks it DEAD
    registry.upsert("S1", kUrl1, {});  // recovers by re-registering

    const auto applied = registry.record_outcomes({DeliveryObservation{stale, false}})[0];
    CHECK(!applied.applied);
    CHECK(registry.find("S1")->state == SubscriberState::ALIVE);
    CHECK(registry.find("S1")->failures == 0);
}

void test_stale_success_after_reregister_not_counted() {
    Registry registry;
    registry.upsert("S1", kUrl1, {});
    const auto stale = snapshot(registry, "S1");
    registry.upsert("S1", kUrl2, {});

    const auto applied = registry.record_outcomes({DeliveryObservation{stale, true}})[0];
    CHECK(!applied.applied);
    CHECK(registry.find("S1")->delivered == 0);
}

void test_batch_mixes_applied_and_stale() {
    Registry registry;
    registry.upsert("S1", kUrl1, {});
    registry.upsert("S2", kUrl1, {});
    const auto s1 = snapshot(registry, "S1");
    const auto s2 = snapshot(registry, "S2");
    registry.remove("S2");

    const auto out = registry.record_outcomes({
        DeliveryObservation{s1, true},
        DeliveryObservation{s2, false},
    });
    CHECK(out.size() == 2);
    CHECK(out.size() == 2 && out[0].applied);
    CHECK(out.size() == 2 && !out[1].applied);
    CHECK(registry.find("S1")->delivered == 1);
}

// Invariant under any interleaving: ALIVE ⇔ failures == 0.
void test_concurrent_reregistration_and_outcomes_keep_invariant() {
    Registry registry;
    registry.upsert("S1", kUrl1, {"sports"});
    registry.upsert("S2", kUrl2, {"sports"});

    constexpr int kIterations = 2000;
    std::vector<std::thread> threads;
    for (int t = 0; t < 3; ++t) {
        threads.emplace_back([&registry, t] {
            for (int i = 0; i < kIterations; ++i) {
                for (const auto& r : registry.multicast_recipients("sports")) {
                    registry.record_outcomes({DeliveryObservation{r, (i + t) % 3 != 0}});
                }
            }
        });
    }
    threads.emplace_back([&registry] {
        for (int i = 0; i < kIterations; ++i) {
            registry.upsert("S1", kUrl1, {"sports"});
            if (i % 50 == 0) registry.remove("S2");
            if (i % 50 == 1) registry.upsert("S2", kUrl2, {"sports"});
        }
    });
    for (auto& th : threads) th.join();

    for (const std::string id : {"S1", "S2"}) {
        const auto stored = registry.find(id);
        CHECK(stored.has_value());
        if (stored) {
            CHECK((stored->state == SubscriberState::ALIVE) == (stored->failures == 0));
        }
    }
}

}  // namespace

int main() {
    test_new_registration_is_alive();
    test_empty_topics_allowed();
    test_reregister_replaces_url_topics_and_resets_state();
    test_reregister_same_url_revives_dead_subscriber();
    test_reregister_changes_generation();
    test_remove();
    test_find_and_list_ordering();
    test_unicast_target_unknown_id();
    test_unicast_target_alive_and_dead();
    test_multicast_selects_alive_subscribers_with_topic();
    test_broadcast_selects_all_alive();
    test_zero_recipients();
    test_parse_publish_mode();
    test_success_on_alive_counts_delivery_only();
    test_late_success_does_not_revive_dead();
    test_earlier_success_after_later_failure_keeps_dead();
    test_failure_marks_dead_and_counts_failures();
    test_result_for_removed_subscriber_not_applied();
    test_stale_failure_after_reregister_same_url_not_applied();
    test_stale_failure_after_revival_does_not_kill_new_registration();
    test_stale_success_after_reregister_not_counted();
    test_batch_mixes_applied_and_stale();
    test_late_successes_never_revive_dead_under_concurrency();
    test_concurrent_reregistration_and_outcomes_keep_invariant();

    if (g_failures == 0) {
        std::printf("registry_test: all checks passed\n");
        return 0;
    }
    std::fprintf(stderr, "registry_test: %d check(s) failed\n", g_failures);
    return 1;
}
