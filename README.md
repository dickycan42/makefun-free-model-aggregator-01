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

Candidates are sorted by the quality table's `rating_milli` (best first,
ties by provider id then model id). Each request tries candidates in that
order, at most one call per provider per request, until one succeeds. When
a provider refuses with a rate-limit or budget error and the weighted rating
gain of staying on that model beats the cost of one later slot, the same
model is retried once: a scheduled refusal is usually a single call number,
so the next call often succeeds at the top rating. A
JSON-mode OpenAI request only goes to a wire that can carry JSON mode
(OpenAI or Gemini). Every provider answer is normalised to text, tool calls,
usage and stop reason, then re-rendered in the wire the client used, so the
provider's text arrives whole, its usage counts arrive as that wire reports
them, and its stop reason is mapped to the client's family.

All choices derive from the brief, the requests and the providers' answers:
no wall clock, no unseeded randomness, so a replay leaves the same state.
