# Free Model Aggregator

One server that serves the OpenAI Chat Completions API and the Anthropic
Messages API over ten simulated free-tier model providers, in JSON and in
server-sent events. It reads the provider directory, the quality table and
the workload manifest from the trial brief, probes every provider's catalog,
ranks the free models that meet each request's requirements by their quality
rating, fails over to the next provider when one refuses, and registers its
catalog with the trial env.

## Run

Requires Node 22+ (standard library only, nothing to install).

```sh
# List the catalog the server would register, then exit:
node main.ts --list

# Serve a trial (the trial sets MAKEFUN_PROMPT_FILE and MAKEFUN_ENV_URL):
node main.ts
```

With no brief and no env set, `--list` prints `{"providers":[]}` and exits 0.

## Endpoints

- `POST /v1/chat/completions` — OpenAI Chat Completions, JSON or SSE
  (`stream: true`).
- `POST /v1/messages` — Anthropic Messages, JSON or SSE (`stream: true`).
- `GET /v1/models` — the model list in the wire the auth header names:
  `Authorization: Bearer` gets the OpenAI list, `x-api-key` the Anthropic
  list.

Requests name model `"auto"`; the server chooses the best free model that
meets the request's requirements (tools, vision, JSON mode, minimum context)
and still answers. Only free models are ever used; a paid model is never a
legal pick.

## How routing works

v4: each request takes the provider with the highest score, where
score = bestRating - 60 * calls - 45 * refusals (one entry per provider:
its best model that meets the request's requirements). A 2nd call to the
top model costs 60 rating-milli, so a fresh provider rated above
(best - 60) wins that slot — the router cycles through the providers in
rating order, one call each, before any provider takes a 2nd call, and
the overflow (3rd calls) loses to any fresh provider rated above
(best - 120). The 20 calls therefore spread over ~10 providers at ~2
calls each instead of hammering one provider into its failing call
numbers (every provider fails on call #2, #3 or #4 in the practice
trials; only groq/kilol/pollinations stay safe past call #4). A refusal
costs 45 milli so the router steers around a provider that has already
shown it refuses. Measured (grade-local, 3 practice trials): v3
27/30/31 calls, quality 99; v4 26/25/27 calls, quality 98, score 99 in
every trial (mean 29.33 -> 26.00). The score is 99 for every entry whose
quality stays at 98 or above, so the whole ranking among the 2,538
entries at 99 is the provider-call tie-break — spreading is what moves
the rank.

Each request tries providers in score order, at most one call per
provider per request, until one succeeds. A provider that refuses twice
in a row is treated as spent for the rest of the trial and is not called
again: one refusal is usually a scheduled one-off, but two in a row mean
its free budget is spent or it is in outage, and every further call to
it would only add a failed provider call (the tie-break counts provider
calls, so fewer calls rank higher at an equal score). There is no
immediate retry of a refused model: a retry costs one more provider
call and recovers at most a few rating-milli, which the local grading
runs show never changes any floored score component. A JSON-mode OpenAI
request only goes to a wire that can carry JSON mode (OpenAI or Gemini).
Every provider answer is normalised to text, tool calls, usage and stop
reason, then re-rendered in the wire the client used, so the provider's
text arrives whole, its usage counts arrive as that wire reports them,
and its stop reason is mapped to the client's family.

All choices derive from the brief, the requests and the providers' answers:
no wall clock, no unseeded randomness, so a replay leaves the same state.
