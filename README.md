# Shunri roundtable

A development tool that lets eight AI reviewers ("seats") build, review and vote on changes to the Shunri app
without anyone watching. A change is kept only when at least 7 of the 8 seats approve it. The app itself does
not need this tool.

How it works is described in [ARCHITECTURE.md](ARCHITECTURE.md).

## What you need

- A Linux machine (the "coordinator") where you can run commands as root, with Python 3.
- A non-root user on that machine who is logged in to the seat services: Claude Code, AdaL, Kylon and the
  GitHub CLI (`gh`).
- Node.js, to run the app's own tests.
- Accounts for the hosted seats: Masky and AskTheW (keys stay in files on the coordinator, see step 2).
- An n8n instance, if runs should start, keep going and stop on their own.
- An open pull request on GitHub that the roundtable pushes to.

## Set up and run

1. Copy `round.py`, `site.example.json` and the `relay/` folder to `/opt/shunri-v3/` on the coordinator. As root,
   copy `site.example.json` to `/etc/shunri-v3/site.json` (mode 600) and fill it in: the non-root user and its
   home folder, the Node and tool paths, the Kylon agent, room and workspace IDs, the repository, the PR number and
   branch, the name and email for the roundtable's commits, and the n8n workflow, credential and machine IDs.
   These values are kept out of the repository. `SHUNRI_V3_SITE` points to another path.
2. As root, create `/etc/shunri-v3/providers.json` (mode 600). It names the file that holds the Masky key, the
   Masky avatar, and the file that holds the AskTheW token and its project. Then sign in to AskTheW: open the
   link that `--begin` prints, sign in, and copy `code` and `state` from the address bar of the page you land on.
   ```sh
   python3 /opt/shunri-v3/round.py askthew-auth --begin
   python3 /opt/shunri-v3/round.py askthew-auth --code <code> --state <state>
   ```
3. Plan a run. `--base` is a copy of the app folder to start from, `--head` its commit, `--until` the last
   work-queue item to do (the queue is in the app's `docs/PRODUCT-DIRECTION.md`), `--hours` the time limit
   (default 6):
   ```sh
   python3 /opt/shunri-v3/round.py plan --until S4 --hours 3 --base <app-folder> --head <commit>
   ```
   Add `--fix 'S2:what to fix'` to do a fix first, or `--resume` to carry over a stopped run's unfinished work.
4. Start the run, either with n8n or by hand.
   - With n8n: every 5 minutes it runs `supervise`, which starts and restarts everything, and it stops the
     machines when the run is finished. `n8n-driver.json` holds `{{PLACEHOLDERS}}`; fill them in from `site.json`,
     then import `driver.json` into n8n and turn it on:
     ```sh
     python3 /opt/shunri-v3/round.py n8n-workflow > driver.json
     ```
   - By hand:
     ```sh
     python3 /opt/shunri-v3/round.py supervise
     ```
   Without n8n, nothing restarts the run after a crash, and the n8n seat does not vote.
5. Follow the run:
   ```sh
   python3 /opt/shunri-v3/round.py status     # current round and the promoted build
   python3 /opt/shunri-v3/round.py metrics    # seats working, pushes, where each piece of work is
   ```
   Each round writes a `report.md` under `/var/lib/shunri-v3/rounds/<round>/`; all reports are also added to
   `/var/lib/shunri-v3/run-report.md`.
6. To give a running run more time: `round.py extend --until S8 --hours 3`. To stop the promoted build and the
   current vote: `round.py stop`.

7. After you edit the work queue table in the direction, the running queue does not change by itself. `round.py sync-queue`
   lists the differences (`metrics` also shows them as `queue_drift`); `round.py sync-queue --apply` rewrites the queue,
   keeping the status of every item whose id, goal and acceptance still match, then run `supervise` again. In the table,
   an item whose Who does not name the coding seats is taken by the coordinator itself and no seat drafts it.

### How a vote is counted

A round passes with at least 7 of the 8 seats approving. A seat that cannot vote at all is not counted and the number
needed drops by one for that round: a seat out of credits, AskTheW when its answer fails after the retries, Prelint when it
never reviewed the pushed head, and AskTheW or Masky when they are still silent ten minutes after every other seat voted.
A rejection is never dropped. AskTheW's decision question reads the files of its project library, not pasted notes, so the
verification record is written there with `write_project_doc`.

A promoted build is served only on `127.0.0.1` (ports 3470 and 3471, in turn). Nothing is merged or made public.

## Check

From the repository root, with no network and no accounts:

```sh
python3 -m py_compile tools/shunri-v3/round.py
SHUNRI_V3_FAKE=1 SHUNRI_V3_ROOT="$(mktemp -d)" python3 tools/shunri-v3/round.py status
```

`SHUNRI_V3_FAKE=1` replaces every seat with a stand-in and keeps all state in `SHUNRI_V3_ROOT`.

## Use it for another project

Everything specific to Shunri comes from the optional `"project"` block of `site.json`; leave it out and the roundtable behaves
exactly as before. To point it at another project, add the block with the project's own values, put the project's direction file
(with a `## Work queue` table whose ids start with `queue_prefix`) in the app tree, and use a new `slug` and `state_root` so the state,
records and proposal names do not mix with another run. Every key is optional; the values below are the defaults.

```json
"project": {
  "name": "Shunri",                       // shown in prompts, records, commit messages
  "slug": "shunri-v3",                    // names of records, proposals, invocations, user agents, /etc and /opt folders
  "state_root": "/var/lib/shunri-v3",
  "direction_path": "docs/PRODUCT-DIRECTION.md",
  "queue_prefix": "S",                    // work item ids S1, S2, ...
  "queue_last": 8,                        // last item number quoted in prompts (S1-S8)
  "protected_paths": ["docs/PRODUCT-DIRECTION.md", "AGENTS.md", ".gitignore"],
  "seat_readonly_files": ["server.mjs", "calendar-service.mjs", "package.json", "scripts/", "assets/"],
  "entry_file": "app/server.mjs",         // started by the isolated boot check ("server.mjs" means the same)
  "test_command": "node --test",
  "prelint_project": "Shunri",
  "product_description": "Shunri scheduling app",
  "health_path": "/api/calendar/status",  // the app's health URL for the boot check and the watchdog
  "direction_contents": "goal, current gaps, default rescheduling rules, work queue",
  "spec_goal": "the user picks a task and gives it a new time; one proposal, Yes / Try another / Cancel.",
  "walkthrough_task": "rescheduling one task",
  "guard_extra_patterns": [],             // push guard: extra regexes, or {"rule": "name", "regex": "..."}
  "guard_allowed_emails": []              // push guard: extra allowed addresses or domains
}
```

`SHUNRI_V3_SITE` (the site.json path) and the `SHUNRI_V3_*` environment names stay the same for every project. For a project whose
health endpoint differs from Shunri's, check `boot_check` in `round.py`, which also expects the calendar status fields.

## Push guard

Before every push of a candidate to the PR branch, and again at promotion, `round.py` scans the whole tree, the commit message and the
commit identity. It looks for API key patterns (Google `AIza`, `sk-`, GitHub `ghp_`/`gho_`/`ghs_`, private key blocks), secret-looking
assignments, personal email addresses (only GitHub `users.noreply.github.com`, `example.com/.org/.net` and `guard_allowed_emails` pass),
absolute `/Users/` and `/home/` paths, `.env` and key files, and any secret-looking environment value of the coordinator (compared by hash).
On a hit nothing is pushed; the unit goes back to a coding seat as a blocking fix and fails after 3 blocks, and `run-report.md` records
only the rule, file and line, never the matched text.
