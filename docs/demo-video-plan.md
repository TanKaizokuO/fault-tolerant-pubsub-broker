# Demo Video Plan

Project: Fault-Tolerant Publish–Subscribe Broker over REST (`docs/spec.md`).
Target length: 5:00. Segments and timings are fixed by the assignment.

Placeholders in `[brackets]` are for the group to fill in. Nothing in this plan names group members or course details.

## Timeline

| Segment | Time | Content | Format |
|---|---|---|---|
| 1 | 0:00–0:20 | Group members and project name | Title card, camera on speakers |
| 2 | 0:20–1:20 | What the system does | Terminal or slide, narration |
| 3 | 1:20–2:20 | Architecture diagram | Hand-drawn diagram, camera on paper |
| 4 | 2:20–4:20 | Live demo (2:00) | Screen recording of tmux session and publisher pane |
| 5 | 4:20–5:00 | What we learned | Camera on speakers, optional limitations slide |

## Before recording

1. Build once, outside the recording: `cmake -S . -B build && cmake --build build -j`.
2. Start the environment: `SKIP_BUILD=1 ./scripts/run_all.sh`. It opens tmux with panes broker, S1, S2, S3, and publisher. Ports 8080, 9001, 9002, 9003 must be free.
3. Confirm `curl http://localhost:8080/subscribers` lists S1, S2, S3 as ALIVE.
4. Rehearse Segment 4 once in this session. Then run `SKIP_BUILD=1 ./scripts/run_all.sh` again for the real take. A session that already ran step 11 of `demo.sh` cannot be reused, and a rehearsal leaves state behind.
5. Put the Segment 4 commands (below) in a text file or the terminal scrollback so they can be pasted without typing on camera.

## Segment 1: Title (0:00–0:20)

**On screen**
- Title card: "Fault-Tolerant Publish–Subscribe Broker over REST".
- Under it: `[Member 1 name]`, `[Member 2 name]`, `[Member 3 name]` (add or remove lines to match the group).

**Narration (about 20 s)**
> "Hello, we are [Member 1 name], [Member 2 name], and [Member 3 name]. Our project is a fault-tolerant publish–subscribe broker built on REST. Over the next five minutes we show what it does, how it is built, and a live run."

## Segment 2: What the system does (0:20–1:20)

**On screen**
- Slide or terminal with three lines: unicast (one subscriber by id), multicast (all subscribers on a topic), broadcast (all live subscribers).
- Small diagram of the lifecycle: subscriber registers → event published → delivered → DEAD after failed attempts → re-registers → ALIVE.

**Narration (about 60 s)**
> "Publish–subscribe lets senders and receivers stay decoupled. A publisher sends an event to one HTTP endpoint on our broker and chooses a mode. Unicast goes to one subscriber by its id. Multicast goes to every subscriber on a topic. Broadcast goes to every live subscriber.
>
> Subscribers register themselves with an id, a callback port, and a list of topics. The broker delivers each event with an HTTP POST and waits for each acknowledgement before it reports back to the publisher.
>
> Delivery is at-least-once, so a subscriber can receive the same event twice. Subscribers remember recent event ids and ignore repeats. If a delivery fails all three attempts, the broker marks that subscriber DEAD and skips it. When the subscriber registers again, it becomes ALIVE and receives events again."

Facts used: `docs/spec.md` §1, §3, §4.3, §5.1, §6.3, §6.4.

## Segment 3: Architecture diagram (1:20–2:20)

**On screen**
- Camera on a hand-drawn diagram. Draw it while narrating if time allows.

**Hand-drawn diagram (draw in this order)**
1. A box at left labeled `Publisher (curl)`.
2. A large central box labeled `Broker C++ :8080`.
3. Three boxes at right, stacked: `S1 Python sports :9001`, `S2 Python news :9002`, `S3 C++ sports :9003`.
4. Solid arrow Publisher → Broker labeled `POST /publish`.
5. Solid arrows Broker → each subscriber labeled `POST /event`.
6. Dashed arrows each subscriber → Broker labeled `POST /subscribe`. Add a legend: solid = event delivery, dashed = registration.
7. Inside the broker box, write three short labels: `registry`, `fan-out threads`, `delivery + retry`.

**Narration (about 60 s)**
> "Here is the architecture. Every box is a separate operating system process. They communicate only over HTTP with JSON bodies. There is no shared memory and no shared file.
>
> The broker is the centre. It is written in C++17 and holds an in-memory registry of subscribers. When an event is published, the broker starts one thread per recipient, so a slow subscriber does not hold up the others. Each delivery has a one-second connect timeout and a two-second read and write timeout. A delivery is attempted up to three times, with short backoff between attempts.
>
> We run three subscribers on localhost. S1 and S2 are Python. S3 is C++. All three use the same REST contract, which shows the broker works with more than one language. Each subscriber registers by posting to slash subscribe, and receives events on slash event.
>
> The broker is also a single point of failure. We come back to that at the end."

Facts used: `docs/spec.md` §2, §3, §4.1, §5, §6.1, §6.2. Verify the timeouts against `broker/delivery.cpp` before recording; the spec is the stated source.

## Segment 4: Live demo (2:20–4:20)

Two minutes is short. Run the commands without the `demo.sh` pauses. The shell output is the proof. Keep narration to one sentence per beat. Steps 8 and 11 of `docs/spec.md` §10 are dropped to fit the time. Step 8 is an optional extra broadcast; add it only if the take is under time.

**Set up**: the five tmux panes are visible. Publisher pane is active.

### Commands (paste in the publisher pane)

```bash
# Beat 1: current state
curl -s http://localhost:8080/subscribers

# Beat 2: unicast
curl -X POST http://localhost:8080/publish -H 'Content-Type: application/json' \
  -d '{"mode":"unicast","target":"S2","payload":{"msg":"hello, S2 only"}}'

# Beat 3: multicast to the sports topic
curl -X POST http://localhost:8080/publish -H 'Content-Type: application/json' \
  -d '{"mode":"multicast","target":"sports","event_id":"score-2-1","payload":{"score":"2-1"}}'

# Beat 4: broadcast with an explicit event_id
curl -X POST http://localhost:8080/publish -H 'Content-Type: application/json' \
  -d '{"mode":"broadcast","event_id":"notice-1","payload":{"notice":"maintenance at 18:00"}}'

# Beat 5: same event_id again (duplicate)
curl -X POST http://localhost:8080/publish -H 'Content-Type: application/json' \
  -d '{"mode":"broadcast","event_id":"notice-1","payload":{"notice":"maintenance at 18:00"}}'

# Beat 6: after Ctrl+C in the S2 pane, broadcast again
curl -X POST http://localhost:8080/publish -H 'Content-Type: application/json' \
  -d '{"mode":"broadcast","payload":{"notice":"S2 is down"}}'

# Beat 7: state after failure
curl -s http://localhost:8080/subscribers

# Beat 9 (recovery): restart S2 in its pane (see note), then check state
curl -s http://localhost:8080/subscribers

# Beat 10: broadcast after recovery
curl -X POST http://localhost:8080/publish -H 'Content-Type: application/json' \
  -d '{"mode":"broadcast","payload":{"notice":"S2 is back"}}'
```

### Beat timing

| Start | Length | Beat | Action | Expected on screen |
|---|---|---|---|---|
| 2:20 | 10 s | Current state | Beat 1 command | S1, S2, S3 ALIVE |
| 2:30 | 15 s | Unicast | Beat 2; point at S2 pane | Only S2 logs the event; report has one DELIVERED |
| 2:45 | 20 s | Multicast | Beat 3; point at S1 and S3 | S1 and S3 log `score-2-1`; S2 silent |
| 3:05 | 10 s | Broadcast | Beat 4 | S1, S2, S3 all log `notice-1`; three DELIVERED |
| 3:15 | 10 s | Duplicate | Beat 5 | Each subscriber logs duplicate ignored; report shows ack `duplicate` |
| 3:25 | 20 s | Failure | Click S2 pane, Ctrl+C; then Beat 6 | Broker log shows failed attempts and `MARK DEAD S2`; S2 FAILED after 3 attempts; S1, S3 DELIVERED |
| 3:45 | 10 s | DEAD state | Beat 7 | S2 DEAD, failures 1 |
| 3:55 | 15 s | Recovery | Restart S2 (note below); broker log shows `REGISTER S2`; Beat 9 | S2 ALIVE, failures 0 |
| 4:10 | 10 s | Post-recovery broadcast | Beat 10 | S1, S2, S3 all DELIVERED |

Total: 120 s. Hold back a sentence if a step runs long. The recovery beats are the last to cut.

**Narration during the demo** (one line per beat, do not read the commands aloud)
- Beat 1: "Three subscribers are registered and alive."
- Beat 2: "Unicast. The broker sends to S2 only, and S2's pane shows the event."
- Beat 3: "Multicast to the sports topic. S1 and S3 receive it. S2 subscribes to news, so it stays silent."
- Beat 4: "Broadcast reaches all three."
- Beat 5: "I send the same event id again. Every subscriber acks duplicate and ignores it."
- Beat 6: "I stop S2. The broker retries three times, then marks it DEAD. S1 and S3 still receive the event."
- Beat 7: "The registry now shows S2 as DEAD."
- Recovery: "S2 registers again, so it returns to ALIVE with zero failures."
- Beat 10: "All three receive the broadcast again."

**Restarting S2 (recovery)**: the S2 pane's shell still exists after Ctrl+C. The start command is stored in the tmux pane option `@start_cmd`. Two options, both to be rehearsed:
- Press Up, then Enter, in the S2 pane, if the shell history contains the start command.
- Or read it with `tmux show-options -p -v -t <S2 pane id> @start_cmd`, then paste it into the S2 pane.

The S2 pane id is shown by `tmux list-panes -t pubsub -F '#{pane_id} #{pane_title}'`.

**Fallback**: if a live step fails, use the recorded clip of the same step, or run `./scripts/demo.sh` from the publisher pane for the full 11 steps. `demo.sh` pauses for Enter and checks each step against the spec.

## Segment 5: What we learned (4:20–5:00)

**On screen**
- Optional: a slide with three bullet points: "timeouts cannot tell slow from dead", "idempotent subscribers make retries safe", "the broker is a single point of failure".

**Narration (about 40 s, each member speaks)**
> "[Member 1 name]: We learned that timeouts and retries can detect a failed subscriber, but cannot tell a slow subscriber from a dead one.
>
> [Member 2 name]: We learned that at-least-once delivery is only safe with idempotent subscribers. The duplicate step showed that dedup by event id works.
>
> [Member 3 name]: We learned the trade-offs of a single broker. It holds state only in memory, a restart empties the registry, and a subscriber that is DEAD misses events because nothing replays them."

Each line above is a placeholder point grounded in `docs/spec.md` §2, §3, §6.4. The group should rewrite each one in its own words and attribute it to the speaker who learned it.

## Caveats for the presenters

- The live demo is tight. The 11-step `demo.sh` takes about 16 s with automated Enter presses, measured once by the README authors. That does not include live narration or typing. Do not claim it as a live timing.
- Exact subscriber log wording is not fully specified. `docs/spec.md` §7 gives examples. Check the real pane text during rehearsal before narrating words from it.
- `demo.sh` and the README describe failure as Ctrl+C in the S2 pane. Spec §10 step 6 says "Kill S2". These are the same action.
- Do not restart the broker alone during the recording. The README notes that event ids restart at `e-1`, and running subscribers may ack new events as `duplicate`. Restart the subscribers too, or use a fresh `run_all.sh` session.
- The demo runs once per session. Start a fresh session with `./scripts/run_all.sh` before the real take.
- Limitations to mention if asked: single broker, no persistence, no replay for DEAD subscribers, no authentication or TLS, localhost only (`docs/spec.md` §2).
