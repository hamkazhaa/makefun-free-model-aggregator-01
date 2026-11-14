# Free-model aggregator — make.fun bounty `free-model-aggregator-01`

- Bounty: https://make.fun/bounty/free-model-aggregator-01
- Prize: $500 USDC, 20 paid places ($100 / $70 / $50 / $20 / $14)
- Deadline: Monday 16 Nov 2026, 09:30 WIB (02:30 UTC)
- Status: **REBUILT AND TESTED WITH THE REAL KIT, NOT SUBMITTED** — no
  GitHub repo created, no entry submitted, nothing signed, nothing paid,
  no wallet connected.

## What it is

One stdlib-only Python 3.13 server (`main.py`, ~1,450 lines, no
third-party packages) that serves an OpenAI-compatible API
(`GET /v1/models`, `POST /v1/chat/completions`) and an
Anthropic-compatible API (`POST /v1/messages`), plus Gemini
`generateContent` / `streamGenerateContent`, over the simulated
free-tier providers under `MAKEFUN_ENV_URL`.

This is a full rewrite (v2.0.0) of the first version, which was only
ever tested against a local mock because the grading kit returned 404
at the time. The real kit now downloads, and this version was built
against it: its `RULES.txt`, `competition.yaml`, practice trials and
the env source (`program/env/server.mjs`) were all read, and the entry
was graded with the kit's own `grade-local --driver process`. The
mock-based v1 would have scored ~0 on the real kit: it probed
endpoints the env does not serve (`/providers`), registered without
`protocol_version`, built the wrong catalog shape, read quality from
fields that do not exist, dropped the system prompt and images, returned
usage 0/0, and never exited — so the trial would have hit the 180 s
wall and scored 0 on every column.

## How it works (real kit contract)

- **Brief.** The prompt file at `$MAKEFUN_PROMPT_FILE` carries three
  JSON documents, found by `schema`: the provider directory
  (`free-model-aggregator-01/providers/v1`, 10 providers with id,
  wire, auth, `key_env`, key, `path` `/p/<id>`), the quality table
  (`free-model-aggregator-01/ratings/v1`, per-model `rating_milli`,
  `context_length`, `tools`, `vision`, `json`, `free`), and the
  workload manifest (`free-model-aggregator-01/workload/v1`, 20
  requests in arrival order, each with `api`, `stream`, `weight`,
  `requires`).
- **Discovery / catalog.** At startup it probes each provider's own
  catalog route (openai/anthropic `GET <base>/v1/models`, gemini
  `GET <base>/v1beta/models`) with that provider's own auth header.
  The catalog is `{"providers": [{id, base_url: "/p/<id>", key_env,
  wire, models: [{id, free, context_length}]}]}` — the exact document
  printed by `python3 main.py --list` and sent in
  `POST $MAKEFUN_ENV_URL/register`. Free-model status comes from the
  ratings row first, the listing second.
- **Registration.** `POST /register` with
  `{"protocol_version": 1, "port", "catalog"}`. The env only serves
  `/register`, `/work`, `/instance`, and providers under `/p/<id>/`;
  there is no `/providers` endpoint.
- **Routing.** Each request is served by arrival order: the manifest
  row for that request gives its requirements (tools / vision / JSON /
  min_context); the fallback derives them from the body (tools
  present, image present, JSON mode, or a body over 5,000 chars ->
  min_context 200000). Candidates are free models meeting the
  requirements, ranked by `rating_milli`, then context length, then
  provider/model id. A JSON-mode request never goes to an
  Anthropic-wire provider (that wire cannot carry JSON mode, so the
  echo check would fail). A client-named model id that matches a real
  model and meets the requirements is tried first (the real env sends
  model `makefun-router`, which matches nothing and is ignored).
- **Failover.** Candidates are tried in ranked order. A 402 marks
  that provider exhausted for the rest of the run; a 429 / 5xx /
  connection failure moves to the next candidate. Exactly one
  provider call answers each request (the scorer checks that).
- **Wire fidelity.** The request is translated to the provider's wire
  (OpenAI / Anthropic / Gemini) and the answer back to the client's
  wire, in JSON and SSE. Every field the env echo-checks is preserved:
  system prompt, user text, tool names, `max_tokens`, stop sequences,
  image bytes (carried verbatim as data URLs / base64 blocks /
  `inlineData`), and JSON mode. The provider's answer text is returned
  whole, with the provider's usage counts and the stop reason mapped
  to the client's wire (`stop` / `end_turn`). Streaming from a
  provider is read fully and re-emitted in the client's wire (OpenAI
  chunks + usage chunk + `[DONE]`; Anthropic `message_start` /
  `content_block_*` / `message_delta` / `message_stop`).
- **Work loop / exit.** After registering, the server polls
  `GET /work` every 100 ms and exits 0 when the env says the workload
  is done. A process still running at the 180 s wall scores 0.
- **Determinism.** Response ids are sequential (`chatcmpl-agg-N`,
  `msg-agg-N`), `created` is 0, no wall clock, no unseeded randomness,
  so a replay with the same providers leaves the same state.
- **Fallback (no prompt file).** With no brief — e.g. the local mock
  env in `tests/` — discovery falls back to the legacy `/providers`
  endpoints and `PROVIDERS_JSON` / `PROVIDERS_FILE` documents, and
  `/work` polling tolerates a 404.

## Run commands

```bash
cd ~/workspace/makefun/free-model-aggregator-01-repo
MAKEFUN_ENV_URL=http://<env> MAKEFUN_PROMPT_FILE=<prompt.txt> python3 main.py --port 8080
python3 main.py --list          # print the catalog document
python3 tests/run_tests.py      # end-to-end tests against the mock env
```

Local Python here is 3.12.3; the code uses only stdlib features
available in Python 3.9+ and targets Python 3.13 as the bounty requires.

## Verification results (15 Nov 2026, ~02:30 WIB)

### Real kit, practice trials — env server + kit scorer (15 Nov 2026, ~02:40 WIB)

`grade-local --driver process` refuses Python bots (the process sandbox
runs node22 only; Python 3.13 runs only inside Docker, and Docker is
not available in this VM), so the trials were run exactly as the
grader does it: the kit's own env server (`program/env/server.mjs`)
with the practice-trial input, the entry as the bot, then the kit's
own scorer (`program/scorer/score.mjs`) on the env's exported
`state.json`. All three practice trials:

| trial | valid | upstream calls | score | quality | served | wire | failover | catalog |
|---|---|---|---|---|---|---|---|---|
| trial-0 | 20/20 | 30 | **99** | 99 | 100 | 100 | 100 | 100 |
| trial-1 | 20/20 | 36 | **99** | 99 | 100 | 100 | 100 | 100 |
| trial-2 | 20/20 | 38 | **99** | 99 | 100 | 100 | 100 | 100 |

All 233 checks passed on trial-0; the bot registered, served all 20
requests, and exited 0 by itself on every trial (no 180 s wall hit).
The 1 quality point is the trial's own failure schedule refusing the
best model (openrouter `z-ai/glm-5.3-flash:free`, 1475) with 429 on
some calls, so those requests fall over to 1470/1465 models — no
routing can avoid a scheduled refusal of the best model, and the
column floors at 99 either way. Scored trials are redrawn per
instance, so these practice scores do not guarantee the same score on
a redrawn trial.

### Local mock tests — `python3 tests/run_tests.py`

**ALL 20 CHECKS PASSED** against `tests/mock_env.py` (3 providers /
4 models):

- `--list` exits 0, is byte-identical across two runs, lists 4 models
  and 3 providers.
- The catalog received by the mock's `/register` is byte-identical to
  the `--list` output, and the registered port matches.
- `GET /v1/models` lists all 4 models.
- OpenAI chat with no model: the first call hits the mock Anthropic
  provider's 429 and fails over to `openai-good`; the second call is
  served by `claude-free` (highest quality, 0.95).
- Anthropic `/v1/messages` returns the Anthropic wire shape with the
  right text.
- OpenAI streaming (SSE) returns the full text and ends with `[DONE]`.
- A request for the 402 model `openai-cheap` fails over to another
  model instead of returning 502.
- Gemini `/v1beta/models/gemini-free:generateContent` returns the
  Gemini wire shape with the right text (model parsed from the URL).
- A tools request is served by a tools-capable model.

### Old version vs the real env (diagnostic, 15 Nov 2026 ~02:25 WIB)

The v1 entry was run against the real env server
(`program/env/server.mjs`, practice trial-0) before rewriting: it
reported 0 models / 0 providers, `/register` failed 400
(`bad_register`, no `protocol_version`), and the server never exited
(killed by a 25 s timeout). That run is what the v2 rewrite fixes.

## Files

- `main.py` — the entry program (Python 3.13, stdlib only)
- `README.md` — this file
- `LICENSE` — MIT, starting `SPDX-License-Identifier: MIT`,
  copyright 2026 dhiandt
- `.makefun/owner.json` — `{"wallet": "FxmXf1NPQE1YyVZYZAEPoDxVzw1YSxRGYAzcmMNQZ3s"}`
- `tests/mock_env.py` — local mock of `MAKEFUN_ENV_URL` (3 providers,
  402 on one model, 429 once on another)
- `tests/run_tests.py` — the end-to-end test suite (20 checks)

## Honest limits — what is NOT verified

- **Scored trials are drawn per trial.** Provider budgets, failure
  schedules, outages, ratings and the workload are redrawn for each
  scored instance; the entry reads all of them from the brief/env at
  runtime, so it adapts, but the practice-trial score does not
  guarantee the same score on a redrawn trial.
- **No real (live) provider was ever called.** All providers in the
  trials are the kit's simulated env; the entry only ever talks to
  localhost.
- **Trial limits not measured under the grader's pod:** 180 s / 1 CPU /
  2048 MB / 64 processes, 3 graded instances, final-60 cutoff. The
  server is single-file stdlib with a thread pool and no heavy work,
  so it should fit, but this was not measured under those limits.

## What remains before submission (NOT done)

1. Create a public GitHub repo owned by the user's own GitHub
   account, created after the bounty opened (14 Nov 2026 02:30 UTC),
   under the MIT licence, with `main.py` in its root (plus
   `.makefun/owner.json` with the account's wallet).
2. Produce the AI agent session transcript for the sessions that
   built it (min 30% of lines, min 5 turns, timestamps within the
   bounty window) and upload it with the submission.
3. Get the user's approval for the make.fun agent-connect flow
   (make.fun/connect, 15-minute code) — an agent wallet cannot be
   used to log in.
4. Submit via make.fun before Mon 16 Nov 2026 09:30 WIB.
   No submission, signing, payment or wallet connection has been
   done in this build.
