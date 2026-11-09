# How the roundtable works

This explains how `round.py` turns the app's work queue into reviewed, voted and promoted builds. For setup and
commands, see [README.md](README.md). The target it follows is the section "How the roundtable keeps working" in
the app's `docs/PRODUCT-DIRECTION.md`.

## The seats

| Seat | What it is | What it does besides voting |
|---|---|---|
| Claude, AdaL, Kylon | coding agents (Claude Code CLI, AdaL CLI, Kylon's agent through a chat thread) | write and fix code, review each other's drafts |
| Masky | hosted AI avatar | writes the screen text and steps for items a user sees; walks the promoted screens as a first-time user |
| AskTheW | hosted Q&A service | turns an item's acceptance into checks; suggests missing tests |
| Celesto | the coordinator machine itself (no AI model) | runs the app's tests on every draft; re-checks the promoted build every 10 minutes |
| Prelint | GitHub review app | reviews each push to the pull request; keeps the decision ledger |
| n8n | scheduler | confirms the run is healthy; starts and stops the machines |

Celesto, Claude, AdaL, Kylon and Masky usually answer within 1-2 minutes ("fast seats"). Prelint, AskTheW and n8n
take longer ("slow seats").

## Pieces

- **Task board** (`board.json`, guarded by a file lock). Every step is a task with the seats allowed to take it.
  A seat claims a task with a lease (a time limit). If the lease runs out, for example because the seat crashed or
  the machine restarted, the task reopens. After 3 failed attempts it is marked failed.
- **Seat loops** (`round.py seat --name X`). Each seat claims a task, does it, records the result and repeats.
  With nothing to claim it does small idle duties (re-checking the promoted build, suggesting tests).
- **Coordinator** (`round.py drive`). Turns the run plan and the work queue into pieces of work, moves each one
  along the board, counts votes, promotes builds one at a time, writes reports and queues follow-up fixes.
- **Supervisor** (`round.py supervise`, called by n8n every 5 minutes). Restarts the coordinator, the seat loops
  and the Kylon gateway when they are not running, and ends the run at its deadline.

## One piece of work

1. **Prepare** (new items only). Masky writes the screen text and steps; AskTheW writes acceptance checks. Drafting
   waits for them for at most 3 minutes.
2. **Split** (new items only). Claude or AdaL may split the item into up to 3 parts that touch different files.
3. **Draft.** A split item: each coding seat writes one part at the same time, and the parts are merged by file. If
   they conflict, or a part fails twice, the item is drafted whole. A whole item: the coding seat whose turn it is
   writes it; if it has no passing draft after 90 seconds, or fails, another coding seat starts on the same task,
   and the first draft that passes the tests wins. Celesto runs the tests on every draft.
4. **Review.** One fast review (Claude or AdaL, never the author) must find nothing blocking before the draft can
   go on. Slow reviews (Kylon, Masky, AskTheW) run at the same time and never hold it up.
5. **Fix.** Blocking points are grouped by file and spread over up to 3 coding seats. The author gets 60 seconds to
   take the first group. A fix still running 6 minutes after the others finished (12 minutes in any case) is also
   given to another seat; the first copy done wins. After 25 minutes of fixing, the draft goes to the vote anyway.
6. **Propose.** The push guard scans the tree, commit message and commit identity for secrets, personal emails and local paths; on a hit nothing is pushed and the draft goes back to a coding seat (rule, file and line only; 3 blocks fail the unit). If another item was promoted in the meantime, the draft is merged onto that newer build and tested
   again. The draft is pushed to the pull request (so Prelint can review it) and a round opens. Only one proposal
   is made per tick (15 seconds), and at most two rounds vote at the same time.
7. **Vote.** Every seat votes on the exact candidate. Celesto votes after booting it in isolation and running the
   tests; n8n votes with its attestation.
8. **Promote.** With 7 approvals the build is started on a private loopback port (127.0.0.1:3470 or 3471, in turn).
   While a seat with no stand-in is out of credits, 6 of the other 7 are enough.

Up to 3 pieces of work run at once (one per coding seat), each on a different queue item, never past the run's
`--until` item. Up to 5 run at once while a coding seat is idle and every open piece is waiting on a vote, a test
or a slow seat.

**Two-tier voting.** Once the fast seats give at least 4 approvals and no rejection, the next proposal may go out
while the slow seats finish voting. Promotion still needs 7 of 8. A newer promoted build is never replaced by an
older one.

## When a vote fails or concerns are left open

- No quorum: the suggestions of the seats that rejected or abstained start a revision, at most 3 per item. An item
  that still fails is recorded with the reason and the queue moves on.
- Concerns of seats that did not approve, Celesto's regressions, Masky's walkthrough and test ideas from Masky and
  AskTheW become follow-up fixes for their item: at most 2 per item, with requests for the same item combined.
- A slow review that finishes before its draft is proposed is fixed first; one that finishes later becomes a
  follow-up fix.
- An older round that fails after a newer one was built on it hands its concerns to a follow-up on the newest
  build instead of being revised.

Each round writes `report.md` with every vote and, for every seat that did not approve, why and what it asked for.

## Error handling

- A vote that errors, times out or is lost to a reboot is asked again up to 2 times, 30 seconds apart.
- Masky answering in prose is asked once more for the JSON ballot. AskTheW is asked again when it routes the
  question elsewhere or cannot yet see the round's record.
- Prelint is reminded with an `@prelint` comment 90 seconds and 8 minutes after a round starts, if it has not
  reviewed yet.
- A push is tried up to 3 times, with a pause between tries.
- A seat whose service says it is out of credits gets no work until the credits return; its tasks go to the other
  seats. While AdaL is out, its seat runs on the shared Claude account instead.
- If the coordinator machine goes down, the n8n workflow starts it again; `supervise` then resumes the run and
  `watch` restarts the promoted build.
- The run ends when the queue reaches `--until` or its hours run out; the n8n workflow then stops all machines.

## Token use

Usually one seat drafts. Each draft gets one fast and a few slow reviews. Checklists, screen text and walkthroughs
are short and made once per item. Celesto uses no AI model. The extra reviews and the split fixes cost less than
the rejected rounds they prevent, each of which would cost eight votes and a revision.
