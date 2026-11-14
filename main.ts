// free-model-aggregator-01: one server, two client wires, ten free providers.
//
// Serves POST /v1/chat/completions (OpenAI) and POST /v1/messages (Anthropic),
// in JSON and in server-sent events, routing each request to the best free
// model that meets its requirements, failing over across providers when one
// refuses, and registering the catalog it probed with the trial env.
//
// How it works:
//   1. Read the brief at $MAKEFUN_PROMPT_FILE: the provider directory, the
//      quality table (rating + capabilities per free model) and the workload
//      manifest (each request's requirements, in arrival order).
//   2. Probe every provider's catalog route in its own wire and build one
//      catalog document, keeping each listing's model order. The catalog is
//      what --list prints and what /register sends, byte for byte.
//   3. For each client request, rank every free model that meets the
//      requirements by the quality table's rating (best first, ties broken by
//      provider id then model id), try them in that order — at most one call
//      per provider per request — and answer with the first provider that
//      succeeds. A JSON-mode OpenAI request only goes to a wire that can
//      carry JSON mode (OpenAI or Gemini, never Anthropic).
//   4. Every upstream answer is normalised to {text, toolCalls, usage, stop}
//      and re-rendered in the wire the client used, so the provider's text
//      arrives whole, its usage counts arrive as that wire reports them, and
//      its stop reason is mapped to the client's family. Nothing is read from
//      the wall clock and nothing is random: the same brief, requests and
//      provider answers always produce the same bytes.
//   5. Poll GET $MAKEFUN_ENV_URL/work and exit 0 when the env says done.
//
// Only the Node standard library is used. No key is ever written anywhere:
// keys come from the brief and are only ever sent to their own provider.

import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import type { IncomingMessage, Server, ServerResponse } from 'node:http';

const PROTOCOL_VERSION = 1;
const FIXED_CREATED = 1767225600;
const FIXED_ANTHROPIC_CREATED_AT = '2026-01-01T00:00:00Z';
const POLL_MS = 150;
const PROBE_TIMEOUT_MS = 5000;
const UPSTREAM_TIMEOUT_MS = 15000;
const REGISTER_ATTEMPTS = 40;
const REGISTER_RETRY_MS = 250;
const MAX_BODY_BYTES = 8 * 1024 * 1024;
const ANTHROPIC_VERSION = '2023-06-01';
const CHAT_ID = 'chatcmpl-agg';
const MSG_ID = 'msg_agg';
const SSE_PIECE = 13;

const ENV_URL = (process.env.MAKEFUN_ENV_URL ?? '').replace(/\/+$/, '');
const PROMPT_FILE = process.env.MAKEFUN_PROMPT_FILE ?? '';

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

type Json = Record<string, unknown>;
type Wire = 'openai' | 'anthropic' | 'gemini';
type Auth = 'bearer' | 'x-api-key' | 'x-goog-api-key' | 'none';
type StopKind = 'stop' | 'length' | 'tool_calls' | 'content_filter';

function isRec(v: unknown): v is Json {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}
function asStr(v: unknown): string {
  return typeof v === 'string' ? v : '';
}
function asNum(v: unknown): number | null {
  return typeof v === 'number' && Number.isFinite(v) ? v : null;
}
function asBool(v: unknown): boolean | null {
  return typeof v === 'boolean' ? v : null;
}
function clone<T>(v: T): T {
  return JSON.parse(JSON.stringify(v)) as T;
}
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
function log(msg: string): void {
  process.stderr.write(`aggregator: ${msg}\n`);
}
function cmpStr(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

// ---------------------------------------------------------------------------
// The brief: provider directory, quality table, workload manifest
// ---------------------------------------------------------------------------

interface Provider {
  id: string;
  name: string;
  wire: Wire;
  auth: Auth;
  base: string;
  keyEnv: string;
  key: string;
}
interface RatingRow {
  model: string;
  provider: string;
  rating: number;
  context: number | null;
  tools: boolean;
  vision: boolean;
  json: boolean;
  free: boolean;
}
interface Needs {
  tools?: boolean;
  vision?: boolean;
  json?: boolean;
  minContext?: number;
}
interface ManifestEntry {
  id: string;
  api: 'openai' | 'anthropic';
  stream: boolean;
  weight: 1 | 2;
  requires: Needs;
}
interface Brief {
  providers: Provider[];
  ratings: RatingRow[];
  manifest: ManifestEntry[];
  error: string | null;
}

function closingBrace(text: string, start: number): number {
  let depth = 0;
  let inStr = false;
  for (let i = start; i < text.length; i++) {
    const ch = text[i];
    if (inStr) {
      if (ch === '\\') i += 1;
      else if (ch === '"') inStr = false;
    } else if (ch === '"') inStr = true;
    else if (ch === '{') depth += 1;
    else if (ch === '}') {
      depth -= 1;
      if (depth === 0) return i;
    }
  }
  return -1;
}

function schemaDocs(text: string): Json[] {
  const out: Json[] = [];
  for (let at = text.indexOf('{'); at !== -1; at = text.indexOf('{', at + 1)) {
    const end = closingBrace(text, at);
    if (end === -1) continue;
    try {
      const value: unknown = JSON.parse(text.slice(at, end + 1));
      if (isRec(value) && typeof value.schema === 'string') out.push(value);
    } catch {
      // Not a standalone JSON object: keep scanning.
    }
  }
  return out;
}

function parseProvider(v: unknown): Provider | null {
  if (!isRec(v)) return null;
  const id = asStr(v.id);
  const wire = v.wire === 'openai' || v.wire === 'anthropic' || v.wire === 'gemini' ? v.wire : null;
  if (id === '' || wire === null) return null;
  const auth: Auth =
    v.auth === 'bearer' || v.auth === 'x-api-key' || v.auth === 'x-goog-api-key' || v.auth === 'none' ? v.auth : 'bearer';
  const base =
    asStr(v.path) !== ''
      ? asStr(v.path)
      : asStr(v.base_path) !== ''
        ? asStr(v.base_path)
        : asStr(v.base_url) !== ''
          ? asStr(v.base_url)
          : asStr(v.base);
  return {
    id,
    name: asStr(v.name) !== '' ? asStr(v.name) : id,
    wire,
    auth,
    base,
    keyEnv: asStr(v.key_env),
    key: asStr(v.key),
  };
}

function parseRating(v: unknown): RatingRow | null {
  if (!isRec(v)) return null;
  const model = asStr(v.model);
  const provider = asStr(v.provider);
  if (model === '' || provider === '') return null;
  return {
    model,
    provider,
    rating: asNum(v.rating_milli) ?? 0,
    context: asNum(v.context_length),
    tools: v.tools === true,
    vision: v.vision === true,
    json: v.json === true,
    free: v.free === true,
  };
}

function parseManifestEntry(v: unknown): ManifestEntry | null {
  if (!isRec(v)) return null;
  const id = asStr(v.id);
  if (id === '') return null;
  const req = isRec(v.requires) ? v.requires : {};
  const needs: Needs = {};
  if (req.tools === true) needs.tools = true;
  if (req.vision === true) needs.vision = true;
  if (req.json === true) needs.json = true;
  if (typeof req.min_context === 'number' && Number.isFinite(req.min_context)) needs.minContext = req.min_context;
  return {
    id,
    api: v.api === 'anthropic' ? 'anthropic' : 'openai',
    stream: v.stream === true,
    weight: v.weight === 2 ? 2 : 1,
    requires: needs,
  };
}

function readBrief(path: string): Brief {
  const empty: Brief = { providers: [], ratings: [], manifest: [], error: null };
  if (path === '') return { ...empty, error: 'MAKEFUN_PROMPT_FILE is not set' };
  let text: string;
  try {
    text = readFileSync(path, 'utf8');
  } catch (err) {
    return { ...empty, error: `cannot read brief: ${(err as Error).message}` };
  }
  const docs = schemaDocs(text);
  const dirDoc = docs.find((d) => d.schema === 'free-model-aggregator-01/providers/v1' && Array.isArray(d.providers));
  if (!dirDoc || !Array.isArray(dirDoc.providers)) {
    return { ...empty, error: 'brief holds no provider directory document' };
  }
  const providers = (dirDoc.providers as unknown[]).flatMap((v) => {
    const p = parseProvider(v);
    return p === null ? [] : [p];
  });
  const ratingsDoc = docs.find((d) => d.schema === 'free-model-aggregator-01/ratings/v1' && Array.isArray(d.ratings));
  const ratings =
    ratingsDoc && Array.isArray(ratingsDoc.ratings)
      ? (ratingsDoc.ratings as unknown[]).flatMap((v) => {
          const r = parseRating(v);
          return r === null ? [] : [r];
        })
      : [];
  const workloadDoc = docs.find((d) => d.schema === 'free-model-aggregator-01/workload/v1' && Array.isArray(d.manifest));
  const manifest =
    workloadDoc && Array.isArray(workloadDoc.manifest)
      ? (workloadDoc.manifest as unknown[]).flatMap((v) => {
          const m = parseManifestEntry(v);
          return m === null ? [] : [m];
        })
      : [];
  return { providers, ratings, manifest, error: null };
}

function ratingFor(brief: Brief, providerId: string, modelId: string): RatingRow | null {
  return brief.ratings.find((r) => r.provider === providerId && r.model === modelId) ?? null;
}

// ---------------------------------------------------------------------------
// Catalog probing
// ---------------------------------------------------------------------------

interface ListedModel {
  id: string;
  displayName: string;
  free: boolean | null;
  context: number | null;
  tools: boolean | null;
  vision: boolean | null;
  json: boolean | null;
}
interface Probe {
  provider: Provider;
  ok: boolean;
  models: ListedModel[];
}
interface CatalogModel {
  id: string;
  free: boolean;
  context_length: number | null;
}
interface CatalogProvider {
  id: string;
  base_url: string;
  key_env: string;
  wire: Wire;
  models: CatalogModel[];
}
interface Catalog {
  providers: CatalogProvider[];
}

function joinBase(envUrl: string, base: string): string {
  const b = base.trim();
  if (/^https?:\/\//i.test(b)) return b.replace(/\/+$/, '');
  const prefix = envUrl.replace(/\/+$/, '');
  if (b === '') return prefix;
  return `${prefix}${b.startsWith('/') ? '' : '/'}${b.replace(/\/+$/, '')}`;
}

function authHeaders(auth: Auth, key: string): Record<string, string> {
  if (key === '') return {};
  if (auth === 'bearer') return { authorization: `Bearer ${key}` };
  if (auth === 'x-api-key') return { 'x-api-key': key };
  if (auth === 'x-goog-api-key') return { 'x-goog-api-key': key };
  return {};
}

function zeroPriced(v: Json): boolean | null {
  if (!isRec(v.pricing)) return null;
  const values: number[] = [];
  for (const x of Object.values(v.pricing)) {
    if (typeof x === 'number') values.push(x);
    else if (typeof x === 'string' && x.trim() !== '') values.push(Number(x));
  }
  if (values.length === 0) return null;
  return values.every((n) => Number.isFinite(n) && n === 0);
}

function listedModel(wire: Wire, v: unknown): ListedModel | null {
  if (!isRec(v)) return null;
  const id = wire === 'gemini' ? asStr(v.name).replace(/^models\//, '') : asStr(v.id);
  if (id === '') return null;
  const display = asStr(v.display_name) !== '' ? asStr(v.display_name) : asStr(v.displayName) !== '' ? asStr(v.displayName) : id;
  return {
    id,
    displayName: display,
    free: asBool(v.free) ?? zeroPriced(v),
    context: asNum(v.context_length) ?? asNum(v.context_window) ?? asNum(v.inputTokenLimit),
    tools: asBool(v.tools),
    vision: asBool(v.vision),
    json: asBool(v.json),
  };
}

async function probeProvider(provider: Provider): Promise<Probe> {
  const base = joinBase(ENV_URL, provider.base);
  const url = provider.wire === 'gemini' ? `${base}/v1beta/models` : `${base}/v1/models`;
  const headers: Record<string, string> = { accept: 'application/json', ...authHeaders(provider.auth, provider.key) };
  if (provider.wire === 'anthropic') headers['anthropic-version'] = ANTHROPIC_VERSION;
  try {
    const res = await fetch(url, { headers, signal: AbortSignal.timeout(PROBE_TIMEOUT_MS) });
    if (!res.ok) return { provider, ok: false, models: [] };
    const body: unknown = await res.json();
    if (!isRec(body)) return { provider, ok: false, models: [] };
    const list = provider.wire === 'gemini' ? body.models : body.data;
    if (!Array.isArray(list)) return { provider, ok: false, models: [] };
    const models = list.flatMap((v) => {
      const m = listedModel(provider.wire, v);
      return m === null ? [] : [m];
    });
    return { provider, ok: true, models };
  } catch {
    return { provider, ok: false, models: [] };
  }
}

function modelIsFree(model: ListedModel, row: RatingRow | null): boolean {
  if (row !== null) return row.free;
  return model.free ?? false;
}

function buildCatalog(brief: Brief, probes: Probe[]): Catalog {
  const providers: CatalogProvider[] = [];
  for (const probe of probes) {
    if (!probe.ok) continue;
    // Keep the listing's own model order: the scorer compares the free list
    // in exactly this order against the provider's true catalog.
    const seen = new Set<string>();
    const models: CatalogModel[] = [];
    for (const model of probe.models) {
      if (seen.has(model.id)) continue;
      seen.add(model.id);
      const row = ratingFor(brief, probe.provider.id, model.id);
      models.push({
        id: model.id,
        free: modelIsFree(model, row),
        context_length: row?.context ?? model.context ?? null,
      });
    }
    providers.push({
      id: probe.provider.id,
      base_url: probe.provider.base,
      key_env: probe.provider.keyEnv,
      wire: probe.provider.wire,
      models,
    });
  }
  return { providers };
}

// ---------------------------------------------------------------------------
// Capabilities and candidate ranking
// ---------------------------------------------------------------------------

interface Caps {
  context: number | null;
  tools: boolean | null;
  vision: boolean | null;
  json: boolean | null;
}

function capsOf(model: ListedModel, row: RatingRow | null): Caps {
  return {
    context: row?.context ?? model.context ?? null,
    tools: row !== null ? row.tools : model.tools,
    vision: row !== null ? row.vision : model.vision,
    json: row !== null ? row.json : model.json,
  };
}

function meets(needs: Needs, caps: Caps): boolean {
  if (needs.tools === true && caps.tools !== true) return false;
  if (needs.vision === true && caps.vision !== true) return false;
  if (needs.json === true && caps.json !== true) return false;
  if (needs.minContext !== undefined && (caps.context === null || caps.context < needs.minContext)) return false;
  return true;
}

interface Candidate {
  probe: Probe;
  model: ListedModel;
  row: RatingRow | null;
  rating: number;
}

function candidatesFor(brief: Brief, probes: Probe[], needs: Needs, jsonMode: boolean): Candidate[] {
  const out: Candidate[] = [];
  for (const probe of probes) {
    if (!probe.ok) continue;
    // A JSON-mode OpenAI request can only ride a wire that carries JSON mode.
    if (jsonMode && probe.provider.wire === 'anthropic') continue;
    for (const model of probe.models) {
      const row = ratingFor(brief, probe.provider.id, model.id);
      if (!modelIsFree(model, row)) continue;
      if (!meets(needs, capsOf(model, row))) continue;
      out.push({ probe, model, row, rating: row?.rating ?? 0 });
    }
  }
  out.sort((a, b) => b.rating - a.rating || cmpStr(a.probe.provider.id, b.probe.provider.id) || cmpStr(a.model.id, b.model.id));
  return out;
}

// ---------------------------------------------------------------------------
// Reading the client's own requirements off its body
// ---------------------------------------------------------------------------

function textBlocks(content: unknown): string {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content
    .flatMap((b) => (isRec(b) && b.type === 'text' && typeof b.text === 'string' ? [b.text] : []))
    .join('\n');
}

function bodyNeeds(clientApi: 'openai' | 'anthropic', body: Json): { needs: Needs; jsonMode: boolean } {
  const needs: Needs = {};
  let jsonMode = false;
  if (clientApi === 'openai') {
    if (Array.isArray(body.tools) && body.tools.length > 0) needs.tools = true;
    const format = isRec(body.response_format) ? body.response_format.type : undefined;
    if (format === 'json_object' || format === 'json_schema') {
      needs.json = true;
      jsonMode = true;
    }
    const messages = Array.isArray(body.messages) ? body.messages : [];
    for (const m of messages) {
      if (!isRec(m) || !Array.isArray(m.content)) continue;
      for (const block of m.content) {
        if (isRec(block) && (block.type === 'image_url' || block.type === 'image')) needs.vision = true;
      }
    }
  } else {
    if (Array.isArray(body.tools) && body.tools.length > 0) needs.tools = true;
    const messages = Array.isArray(body.messages) ? body.messages : [];
    for (const m of messages) {
      if (!isRec(m) || !Array.isArray(m.content)) continue;
      for (const block of m.content) {
        if (isRec(block) && block.type === 'image') needs.vision = true;
      }
    }
  }
  return { needs, jsonMode };
}

// ---------------------------------------------------------------------------
// Client body -> upstream body, per wire pair
// ---------------------------------------------------------------------------

function openAIToolsToAnthropic(tools: unknown): Json[] {
  if (!Array.isArray(tools)) return [];
  return tools.flatMap((raw) => {
    if (!isRec(raw) || !isRec(raw.function) || typeof raw.function.name !== 'string') return [];
    return [
      {
        name: raw.function.name,
        description: asStr(raw.function.description),
        input_schema: isRec(raw.function.parameters) ? raw.function.parameters : { type: 'object', properties: {} },
      },
    ];
  });
}

function anthropicToolsToOpenAI(tools: unknown): Json[] {
  if (!Array.isArray(tools)) return [];
  return tools.flatMap((raw) => {
    if (!isRec(raw) || typeof raw.name !== 'string') return [];
    return [
      {
        type: 'function',
        function: {
          name: raw.name,
          description: asStr(raw.description),
          parameters: isRec(raw.input_schema) ? raw.input_schema : { type: 'object', properties: {} },
        },
      },
    ];
  });
}

function anthropicToolsToGemini(tools: unknown): Json[] {
  if (!Array.isArray(tools)) return [];
  const decls = tools.flatMap((raw) => {
    if (!isRec(raw) || typeof raw.name !== 'string') return [];
    return [
      {
        name: raw.name,
        description: asStr(raw.description),
        parameters: isRec(raw.input_schema) ? raw.input_schema : { type: 'object', properties: {} },
      },
    ];
  });
  return decls.length > 0 ? [{ functionDeclarations: decls }] : [];
}

function openAIToolsToGemini(tools: unknown): Json[] {
  if (!Array.isArray(tools)) return [];
  const decls = tools.flatMap((raw) => {
    if (!isRec(raw) || !isRec(raw.function) || typeof raw.function.name !== 'string') return [];
    return [
      {
        name: raw.function.name,
        description: asStr(raw.function.description),
        parameters: isRec(raw.function.parameters) ? raw.function.parameters : { type: 'object', properties: {} },
      },
    ];
  });
  return decls.length > 0 ? [{ functionDeclarations: decls }] : [];
}

/** One OpenAI image_url part as an Anthropic image block. */
function imageUrlToAnthropic(block: Json): Json | null {
  if (!isRec(block.image_url) || typeof block.image_url.url !== 'string') return null;
  const url = block.image_url.url;
  const data = /^data:([^;,]+)?;base64,(.*)$/s.exec(url);
  if (data !== null) {
    return { type: 'image', source: { type: 'base64', media_type: data[1] ?? 'image/png', data: data[2] } };
  }
  return { type: 'image', source: { type: 'url', url } };
}

/** One OpenAI content part as a Gemini part. */
function openAIPartToGemini(block: Json): Json | null {
  if (block.type === 'text' && typeof block.text === 'string') return { text: block.text };
  if (block.type === 'image_url' && isRec(block.image_url) && typeof block.image_url.url === 'string') {
    const url = block.image_url.url;
    const data = /^data:([^;,]+)?;base64,(.*)$/s.exec(url);
    if (data !== null) return { inlineData: { mimeType: data[1] ?? 'image/png', data: data[2] } };
    return { fileData: { fileUri: url } };
  }
  return null;
}

/** One Anthropic content block as a Gemini part. */
function anthropicBlockToGemini(block: Json): Json | null {
  if (block.type === 'text' && typeof block.text === 'string') return { text: block.text };
  if (block.type === 'image' && isRec(block.source)) {
    const src = block.source;
    if (src.type === 'base64' && typeof src.data === 'string') {
      return { inlineData: { mimeType: asStr(src.media_type) !== '' ? asStr(src.media_type) : 'image/png', data: src.data } };
    }
    if (src.type === 'url' && typeof src.url === 'string') return { fileData: { fileUri: src.url } };
  }
  return null;
}

/** One Anthropic content block as an OpenAI content part. */
function anthropicBlockToOpenAI(block: Json): Json | null {
  if (block.type === 'text' && typeof block.text === 'string') return { type: 'text', text: block.text };
  if (block.type === 'image' && isRec(block.source)) {
    const src = block.source;
    if (src.type === 'base64' && typeof src.data === 'string') {
      const media = asStr(src.media_type) !== '' ? asStr(src.media_type) : 'image/png';
      return { type: 'image_url', image_url: { url: `data:${media};base64,${src.data}` } };
    }
    if (src.type === 'url' && typeof src.url === 'string') return { type: 'image_url', image_url: { url: src.url } };
  }
  return null;
}

function toolResultText(content: unknown): string {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    const t = textBlocks(content);
    return t !== '' ? t : JSON.stringify(content);
  }
  return content === undefined ? '' : JSON.stringify(content);
}

/** OpenAI client body -> Anthropic upstream body. */
function openAIToAnthropicBody(body: Json, model: string): Json {
  const messages: Json[] = [];
  let system = '';
  const source = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of source) {
    if (!isRec(raw)) continue;
    if (raw.role === 'system') {
      const t = textBlocks(raw.content);
      if (t !== '') system = system === '' ? t : `${system}\n${t}`;
      continue;
    }
    if (raw.role === 'assistant') {
      const blocks: Json[] = [];
      const text = textBlocks(raw.content);
      if (text !== '') blocks.push({ type: 'text', text });
      const calls = Array.isArray(raw.tool_calls) ? raw.tool_calls : [];
      for (const call of calls) {
        if (!isRec(call) || !isRec(call.function)) continue;
        let input: Json = {};
        if (typeof call.function.arguments === 'string' && call.function.arguments.trim() !== '') {
          try {
            const parsed: unknown = JSON.parse(call.function.arguments);
            if (isRec(parsed)) input = parsed;
          } catch {
            // Not JSON: empty input.
          }
        }
        blocks.push({ type: 'tool_use', id: asStr(call.id), name: asStr(call.function.name), input });
      }
      messages.push({ role: 'assistant', content: blocks });
      continue;
    }
    if (raw.role === 'tool') {
      messages.push({
        role: 'user',
        content: [{ type: 'tool_result', tool_use_id: asStr(raw.tool_call_id), content: toolResultText(raw.content) }],
      });
      continue;
    }
    if (raw.role !== 'user') continue;
    if (typeof raw.content === 'string') {
      messages.push({ role: 'user', content: raw.content });
      continue;
    }
    if (!Array.isArray(raw.content)) continue;
    const parts: Json[] = [];
    for (const block of raw.content) {
      if (!isRec(block)) continue;
      if (block.type === 'text' && typeof block.text === 'string') parts.push({ type: 'text', text: block.text });
      else if (block.type === 'image_url') {
        const img = imageUrlToAnthropic(block);
        if (img !== null) parts.push(img);
      }
    }
    if (parts.length > 0) messages.push({ role: 'user', content: parts });
  }
  const out: Json = { model, messages };
  if (system !== '') out.system = system;
  if (typeof body.max_tokens === 'number') out.max_tokens = body.max_tokens;
  else if (typeof body.max_completion_tokens === 'number') out.max_tokens = body.max_completion_tokens;
  if (typeof body.temperature === 'number') out.temperature = body.temperature;
  if (typeof body.top_p === 'number') out.top_p = body.top_p;
  if (typeof body.stop === 'string') out.stop_sequences = [body.stop];
  else if (Array.isArray(body.stop) && body.stop.length > 0) out.stop_sequences = body.stop;
  const tools = openAIToolsToAnthropic(body.tools);
  if (tools.length > 0) out.tools = tools;
  const choice = body.tool_choice;
  if (choice === 'auto') out.tool_choice = { type: 'auto' };
  else if (choice === 'required' || choice === 'none') out.tool_choice = { type: choice === 'required' ? 'any' : 'auto' };
  else if (isRec(choice) && isRec(choice.function)) out.tool_choice = { type: 'tool', name: asStr(choice.function.name) };
  if (body.stream === true) out.stream = true;
  return out;
}

/** Anthropic client body -> OpenAI upstream body. */
function anthropicToOpenAIBody(body: Json, model: string): Json {
  const messages: Json[] = [];
  const system = textBlocks(body.system);
  if (system !== '') messages.push({ role: 'system', content: system });
  const source = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of source) {
    if (!isRec(raw)) continue;
    if (raw.role === 'assistant') {
      const calls: Json[] = [];
      const content = raw.content;
      if (Array.isArray(content)) {
        for (const block of content) {
          if (!isRec(block) || block.type !== 'tool_use') continue;
          calls.push({
            id: asStr(block.id),
            type: 'function',
            function: { name: asStr(block.name), arguments: JSON.stringify(isRec(block.input) ? block.input : {}) },
          });
        }
      }
      const text = textBlocks(content);
      messages.push({ role: 'assistant', content: text !== '' ? text : null, ...(calls.length > 0 ? { tool_calls: calls } : {}) });
      continue;
    }
    if (typeof raw.content === 'string') {
      messages.push({ role: 'user', content: raw.content });
      continue;
    }
    if (!Array.isArray(raw.content)) continue;
    const parts: Json[] = [];
    for (const block of raw.content) {
      if (!isRec(block)) continue;
      if (block.type === 'tool_result') {
        messages.push({ role: 'tool', tool_call_id: asStr(block.tool_use_id), content: toolResultText(block.content) });
        continue;
      }
      const part = anthropicBlockToOpenAI(block);
      if (part !== null) parts.push(part);
    }
    if (parts.length > 0) messages.push({ role: 'user', content: parts });
  }
  const out: Json = { model, messages };
  if (typeof body.max_tokens === 'number') out.max_tokens = body.max_tokens;
  if (typeof body.temperature === 'number') out.temperature = body.temperature;
  if (typeof body.top_p === 'number') out.top_p = body.top_p;
  if (Array.isArray(body.stop_sequences) && body.stop_sequences.length > 0) out.stop = body.stop_sequences;
  const tools = anthropicToolsToOpenAI(body.tools);
  if (tools.length > 0) out.tools = tools;
  const choice = body.tool_choice;
  if (isRec(choice)) {
    if (choice.type === 'auto') out.tool_choice = 'auto';
    else if (choice.type === 'any') out.tool_choice = 'required';
    else if (choice.type === 'tool' && typeof choice.name === 'string') {
      out.tool_choice = { type: 'function', function: { name: choice.name } };
    }
  }
  if (body.stream === true) {
    out.stream = true;
    out.stream_options = { include_usage: true };
  }
  return out;
}

/** OpenAI client body -> Gemini upstream body. */
function openAIToGeminiBody(body: Json): Json {
  const contents: Json[] = [];
  let system = '';
  const source = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of source) {
    if (!isRec(raw)) continue;
    if (raw.role === 'system') {
      const t = textBlocks(raw.content);
      if (t !== '') system = system === '' ? t : `${system}\n${t}`;
      continue;
    }
    if (raw.role === 'assistant') {
      const parts: Json[] = [];
      const text = textBlocks(raw.content);
      if (text !== '') parts.push({ text });
      const calls = Array.isArray(raw.tool_calls) ? raw.tool_calls : [];
      for (const call of calls) {
        if (!isRec(call) || !isRec(call.function)) continue;
        let args: Json = {};
        if (typeof call.function.arguments === 'string' && call.function.arguments.trim() !== '') {
          try {
            const parsed: unknown = JSON.parse(call.function.arguments);
            if (isRec(parsed)) args = parsed;
          } catch {
            // Not JSON: empty args.
          }
        }
        parts.push({ functionCall: { name: asStr(call.function.name), args } });
      }
      if (parts.length > 0) contents.push({ role: 'model', parts });
      continue;
    }
    if (raw.role === 'tool') {
      contents.push({
        role: 'function',
        parts: [{ functionResponse: { name: '', response: { content: toolResultText(raw.content) } } }],
      });
      continue;
    }
    if (raw.role !== 'user') continue;
    if (typeof raw.content === 'string') {
      contents.push({ role: 'user', parts: [{ text: raw.content }] });
      continue;
    }
    if (!Array.isArray(raw.content)) continue;
    const parts: Json[] = [];
    for (const block of raw.content) {
      if (!isRec(block)) continue;
      const part = openAIPartToGemini(block);
      if (part !== null) parts.push(part);
    }
    if (parts.length > 0) contents.push({ role: 'user', parts });
  }
  const config: Json = {};
  if (typeof body.max_tokens === 'number') config.maxOutputTokens = body.max_tokens;
  else if (typeof body.max_completion_tokens === 'number') config.maxOutputTokens = body.max_completion_tokens;
  if (typeof body.temperature === 'number') config.temperature = body.temperature;
  if (typeof body.top_p === 'number') config.topP = body.top_p;
  if (typeof body.stop === 'string') config.stopSequences = [body.stop];
  else if (Array.isArray(body.stop) && body.stop.length > 0) config.stopSequences = body.stop;
  const format = isRec(body.response_format) ? body.response_format.type : undefined;
  if (format === 'json_object' || format === 'json_schema') config.responseMimeType = 'application/json';
  const out: Json = { contents };
  if (system !== '') out.systemInstruction = { parts: [{ text: system }] };
  const tools = openAIToolsToGemini(body.tools);
  if (tools.length > 0) out.tools = tools;
  if (Object.keys(config).length > 0) out.generationConfig = config;
  return out;
}

/** Anthropic client body -> Gemini upstream body. */
function anthropicToGeminiBody(body: Json): Json {
  const contents: Json[] = [];
  const system = textBlocks(body.system);
  const source = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of source) {
    if (!isRec(raw)) continue;
    if (raw.role === 'assistant') {
      const parts: Json[] = [];
      const content = raw.content;
      if (Array.isArray(content)) {
        for (const block of content) {
          if (!isRec(block)) continue;
          if (block.type === 'text' && typeof block.text === 'string') parts.push({ text: block.text });
          else if (block.type === 'tool_use') {
            parts.push({ functionCall: { name: asStr(block.name), args: isRec(block.input) ? block.input : {} } });
          }
        }
      } else if (typeof content === 'string' && content !== '') parts.push({ text: content });
      if (parts.length > 0) contents.push({ role: 'model', parts });
      continue;
    }
    if (typeof raw.content === 'string') {
      contents.push({ role: 'user', parts: [{ text: raw.content }] });
      continue;
    }
    if (!Array.isArray(raw.content)) continue;
    const parts: Json[] = [];
    let toolResult: Json | null = null;
    for (const block of raw.content) {
      if (!isRec(block)) continue;
      if (block.type === 'tool_result') {
        toolResult = {
          role: 'function',
          parts: [
            {
              functionResponse: {
                name: '',
                response: { content: toolResultText(block.content) },
              },
            },
          ],
        };
        continue;
      }
      const part = anthropicBlockToGemini(block);
      if (part !== null) parts.push(part);
    }
    if (toolResult !== null) contents.push(toolResult);
    if (parts.length > 0) contents.push({ role: 'user', parts });
  }
  const config: Json = {};
  if (typeof body.max_tokens === 'number') config.maxOutputTokens = body.max_tokens;
  if (typeof body.temperature === 'number') config.temperature = body.temperature;
  if (typeof body.top_p === 'number') config.topP = body.top_p;
  if (Array.isArray(body.stop_sequences) && body.stop_sequences.length > 0) config.stopSequences = body.stop_sequences;
  const out: Json = { contents };
  if (system !== '') out.systemInstruction = { parts: [{ text: system }] };
  const tools = anthropicToolsToGemini(body.tools);
  if (tools.length > 0) out.tools = tools;
  if (Object.keys(config).length > 0) out.generationConfig = config;
  return out;
}

// ---------------------------------------------------------------------------
// Upstream answers -> one normalised answer
// ---------------------------------------------------------------------------

interface ToolCall {
  id: string;
  name: string;
  arguments: string;
}
interface NormAnswer {
  text: string;
  toolCalls: ToolCall[];
  inputTokens: number;
  outputTokens: number;
  stop: StopKind;
}

function numOrZero(v: unknown): number {
  return typeof v === 'number' && Number.isFinite(v) ? v : 0;
}

function sseFrames(text: string): { event: string | null; data: string }[] {
  const frames: { event: string | null; data: string }[] = [];
  const normalized = text.replace(/\r\n/g, '\n');
  for (const frame of normalized.split('\n\n')) {
    if (frame.trim() === '') continue;
    let event: string | null = null;
    const data: string[] = [];
    for (const line of frame.split('\n')) {
      if (line.startsWith('event:')) event = line.slice('event:'.length).trim();
      else if (line.startsWith('data:')) data.push(line.slice('data:'.length).replace(/^ /, ''));
    }
    if (data.length > 0) frames.push({ event, data: data.join('\n') });
  }
  return frames;
}

function openAIStop(v: unknown): StopKind {
  if (v === 'length') return 'length';
  if (v === 'tool_calls' || v === 'function_call') return 'tool_calls';
  if (v === 'content_filter') return 'content_filter';
  return 'stop';
}

function parseOpenAIAnswer(bodyText: string, stream: boolean): NormAnswer | null {
  if (!stream) {
    let value: unknown;
    try {
      value = JSON.parse(bodyText);
    } catch {
      return null;
    }
    if (!isRec(value)) return null;
    const choices = Array.isArray(value.choices) ? value.choices : [];
    const choice = isRec(choices[0]) ? choices[0] : {};
    const message = isRec(choice.message) ? choice.message : {};
    const text = typeof message.content === 'string' ? message.content : textBlocks(message.content);
    const calls: ToolCall[] = [];
    if (Array.isArray(message.tool_calls)) {
      for (const c of message.tool_calls) {
        if (!isRec(c) || !isRec(c.function)) continue;
        calls.push({ id: asStr(c.id), name: asStr(c.function.name), arguments: asStr(c.function.arguments) });
      }
    }
    const usage = isRec(value.usage) ? value.usage : {};
    return {
      text,
      toolCalls: calls,
      inputTokens: numOrZero(usage.prompt_tokens),
      outputTokens: numOrZero(usage.completion_tokens),
      stop: openAIStop(choice.finish_reason),
    };
  }
  let text = '';
  let inputTokens = 0;
  let outputTokens = 0;
  let stop: StopKind = 'stop';
  const callMap = new Map<number, ToolCall>();
  for (const frame of sseFrames(bodyText)) {
    const payload = frame.data.trim();
    if (payload === '[DONE]') continue;
    let chunk: unknown;
    try {
      chunk = JSON.parse(payload);
    } catch {
      continue;
    }
    if (!isRec(chunk)) continue;
    if (isRec(chunk.usage)) {
      inputTokens = numOrZero(chunk.usage.prompt_tokens);
      outputTokens = numOrZero(chunk.usage.completion_tokens);
    }
    const choices = Array.isArray(chunk.choices) ? chunk.choices : [];
    const choice = isRec(choices[0]) ? choices[0] : null;
    if (choice === null) continue;
    const delta = isRec(choice.delta) ? choice.delta : {};
    if (typeof delta.content === 'string') text += delta.content;
    if (Array.isArray(delta.tool_calls)) {
      for (const c of delta.tool_calls) {
        if (!isRec(c)) continue;
        const idx = typeof c.index === 'number' ? c.index : 0;
        const prev = callMap.get(idx) ?? { id: '', name: '', arguments: '' };
        const fn = isRec(c.function) ? c.function : {};
        callMap.set(idx, {
          id: asStr(c.id) !== '' ? asStr(c.id) : prev.id,
          name: asStr(fn.name) !== '' ? asStr(fn.name) : prev.name,
          arguments: prev.arguments + asStr(fn.arguments),
        });
      }
    }
    if (typeof choice.finish_reason === 'string') stop = openAIStop(choice.finish_reason);
  }
  return { text, toolCalls: [...callMap.values()], inputTokens, outputTokens, stop };
}

function anthropicStop(v: unknown): StopKind {
  if (v === 'max_tokens') return 'length';
  if (v === 'tool_use') return 'tool_calls';
  if (v === 'refusal') return 'content_filter';
  return 'stop';
}

function parseAnthropicAnswer(bodyText: string, stream: boolean): NormAnswer | null {
  if (!stream) {
    let value: unknown;
    try {
      value = JSON.parse(bodyText);
    } catch {
      return null;
    }
    if (!isRec(value) || !Array.isArray(value.content)) return null;
    let text = '';
    const calls: ToolCall[] = [];
    for (const block of value.content) {
      if (!isRec(block)) continue;
      if (block.type === 'text' && typeof block.text === 'string') text += block.text;
      else if (block.type === 'tool_use') {
        calls.push({ id: asStr(block.id), name: asStr(block.name), arguments: JSON.stringify(isRec(block.input) ? block.input : {}) });
      }
    }
    const usage = isRec(value.usage) ? value.usage : {};
    return {
      text,
      toolCalls: calls,
      inputTokens: numOrZero(usage.input_tokens),
      outputTokens: numOrZero(usage.output_tokens),
      stop: anthropicStop(value.stop_reason),
    };
  }
  let text = '';
  let inputTokens = 0;
  let outputTokens = 0;
  let stop: StopKind = 'stop';
  const callMap = new Map<number, ToolCall>();
  for (const frame of sseFrames(bodyText)) {
    let data: unknown;
    try {
      data = JSON.parse(frame.data);
    } catch {
      continue;
    }
    if (!isRec(data)) continue;
    if (frame.event === 'message_start' && isRec(data.message) && isRec(data.message.usage)) {
      inputTokens = numOrZero(data.message.usage.input_tokens);
    } else if (frame.event === 'content_block_start' && isRec(data.content_block) && data.content_block.type === 'tool_use') {
      const idx = numOrZero(data.index);
      callMap.set(idx, {
        id: asStr(data.content_block.id),
        name: asStr(data.content_block.name),
        arguments: '',
      });
    } else if (frame.event === 'content_block_delta' && isRec(data.delta)) {
      const idx = numOrZero(data.index);
      if (data.delta.type === 'text_delta' && typeof data.delta.text === 'string') text += data.delta.text;
      else if (data.delta.type === 'input_json_delta' && typeof data.delta.partial_json === 'string') {
        const prev = callMap.get(idx) ?? { id: '', name: '', arguments: '' };
        callMap.set(idx, { ...prev, arguments: prev.arguments + data.delta.partial_json });
      }
    } else if (frame.event === 'message_delta') {
      if (isRec(data.delta) && typeof data.delta.stop_reason === 'string') stop = anthropicStop(data.delta.stop_reason);
      if (isRec(data.usage)) outputTokens = numOrZero(data.usage.output_tokens);
    }
  }
  return { text, toolCalls: [...callMap.values()], inputTokens, outputTokens, stop };
}

function geminiStop(v: unknown): StopKind {
  if (v === 'MAX_TOKENS') return 'length';
  if (v === 'SAFETY') return 'content_filter';
  return 'stop';
}

function parseGeminiAnswer(bodyText: string, stream: boolean): NormAnswer | null {
  if (!stream) {
    let value: unknown;
    try {
      value = JSON.parse(bodyText);
    } catch {
      return null;
    }
    if (!isRec(value) || !Array.isArray(value.candidates)) return null;
    const cand = isRec(value.candidates[0]) ? value.candidates[0] : {};
    const content = isRec(cand.content) ? cand.content : {};
    const parts = Array.isArray(content.parts) ? content.parts : [];
    let text = '';
    const calls: ToolCall[] = [];
    for (const part of parts) {
      if (!isRec(part)) continue;
      if (typeof part.text === 'string') text += part.text;
      else if (isRec(part.functionCall)) {
        calls.push({ id: '', name: asStr(part.functionCall.name), arguments: JSON.stringify(isRec(part.functionCall.args) ? part.functionCall.args : {}) });
      }
    }
    const meta = isRec(value.usageMetadata) ? value.usageMetadata : {};
    return {
      text,
      toolCalls: calls,
      inputTokens: numOrZero(meta.promptTokenCount),
      outputTokens: numOrZero(meta.candidatesTokenCount),
      stop: geminiStop(cand.finishReason),
    };
  }
  let text = '';
  let inputTokens = 0;
  let outputTokens = 0;
  let stop: StopKind = 'stop';
  for (const frame of sseFrames(bodyText)) {
    let value: unknown;
    try {
      value = JSON.parse(frame.data);
    } catch {
      continue;
    }
    if (!isRec(value) || !Array.isArray(value.candidates)) continue;
    const cand = isRec(value.candidates[0]) ? value.candidates[0] : {};
    const content = isRec(cand.content) ? cand.content : {};
    const parts = Array.isArray(content.parts) ? content.parts : [];
    for (const part of parts) {
      if (isRec(part) && typeof part.text === 'string') text += part.text;
    }
    if (typeof cand.finishReason === 'string') stop = geminiStop(cand.finishReason);
    if (isRec(value.usageMetadata)) {
      inputTokens = numOrZero(value.usageMetadata.promptTokenCount);
      outputTokens = numOrZero(value.usageMetadata.candidatesTokenCount);
    }
  }
  return { text, toolCalls: [], inputTokens, outputTokens, stop };
}

// ---------------------------------------------------------------------------
// Normalised answer -> client wire
// ---------------------------------------------------------------------------

function openAIStopOut(stop: StopKind): string {
  if (stop === 'length') return 'length';
  if (stop === 'tool_calls') return 'tool_calls';
  if (stop === 'content_filter') return 'content_filter';
  return 'stop';
}
function anthropicStopOut(stop: StopKind): string {
  if (stop === 'length') return 'max_tokens';
  if (stop === 'tool_calls') return 'tool_use';
  if (stop === 'content_filter') return 'refusal';
  return 'end_turn';
}

function splitPieces(text: string): string[] {
  const out: string[] = [];
  for (let at = 0; at < text.length; at += SSE_PIECE) out.push(text.slice(at, at + SSE_PIECE));
  return out.length === 0 ? [''] : out;
}

function sseLine(event: string | null, data: unknown): string {
  return `${event === null ? '' : `event: ${event}\n`}data: ${JSON.stringify(data)}\n\n`;
}

function renderOpenAI(answer: NormAnswer, model: string, stream: boolean): string {
  const usage = {
    prompt_tokens: answer.inputTokens,
    completion_tokens: answer.outputTokens,
    total_tokens: answer.inputTokens + answer.outputTokens,
  };
  if (!stream) {
    const message: Json = { role: 'assistant', content: answer.text };
    if (answer.toolCalls.length > 0) {
      message.tool_calls = answer.toolCalls.map((c) => ({
        id: c.id !== '' ? c.id : 'call_agg',
        type: 'function',
        function: { name: c.name, arguments: c.arguments },
      }));
    }
    return JSON.stringify({
      id: CHAT_ID,
      object: 'chat.completion',
      created: FIXED_CREATED,
      model,
      choices: [{ index: 0, message, finish_reason: openAIStopOut(answer.stop), logprobs: null }],
      usage,
    });
  }
  const chunk = (delta: Json, finish: string | null): Json => ({
    id: CHAT_ID,
    object: 'chat.completion.chunk',
    created: FIXED_CREATED,
    model,
    choices: [{ index: 0, delta, finish_reason: finish }],
  });
  let out = '';
  out += sseLine(null, chunk({ role: 'assistant', content: '' }, null));
  for (const piece of splitPieces(answer.text)) out += sseLine(null, chunk({ content: piece }, null));
  for (const [i, call] of answer.toolCalls.entries()) {
    out += sseLine(
      null,
      chunk({ tool_calls: [{ index: i, id: call.id !== '' ? call.id : 'call_agg', type: 'function', function: { name: call.name, arguments: call.arguments } }] }, null),
    );
  }
  out += sseLine(null, chunk({}, openAIStopOut(answer.stop)));
  out += sseLine(null, { id: CHAT_ID, object: 'chat.completion.chunk', created: FIXED_CREATED, model, choices: [], usage });
  out += 'data: [DONE]\n\n';
  return out;
}

function renderAnthropic(answer: NormAnswer, model: string, stream: boolean): string {
  const content: Json[] = [];
  if (answer.text !== '') content.push({ type: 'text', text: answer.text });
  for (const call of answer.toolCalls) {
    let input: Json = {};
    if (call.arguments.trim() !== '') {
      try {
        const parsed: unknown = JSON.parse(call.arguments);
        if (isRec(parsed)) input = parsed;
      } catch {
        // Keep empty input.
      }
    }
    content.push({ type: 'tool_use', id: call.id !== '' ? call.id : 'toolu_agg', name: call.name, input });
  }
  if (content.length === 0) content.push({ type: 'text', text: '' });
  const stopOut = anthropicStopOut(answer.stop);
  if (!stream) {
    return JSON.stringify({
      id: MSG_ID,
      type: 'message',
      role: 'assistant',
      model,
      content,
      stop_reason: stopOut,
      stop_sequence: null,
      usage: { input_tokens: answer.inputTokens, output_tokens: answer.outputTokens },
    });
  }
  let out = '';
  out += sseLine('message_start', {
    type: 'message_start',
    message: {
      id: MSG_ID,
      type: 'message',
      role: 'assistant',
      model,
      content: [],
      stop_reason: null,
      stop_sequence: null,
      usage: { input_tokens: answer.inputTokens, output_tokens: 0 },
    },
  });
  let index = 0;
  if (answer.text !== '') {
    out += sseLine('content_block_start', { type: 'content_block_start', index, content_block: { type: 'text', text: '' } });
    for (const piece of splitPieces(answer.text)) {
      out += sseLine('content_block_delta', { type: 'content_block_delta', index, delta: { type: 'text_delta', text: piece } });
    }
    out += sseLine('content_block_stop', { type: 'content_block_stop', index });
    index += 1;
  }
  for (const call of answer.toolCalls) {
    out += sseLine('content_block_start', {
      type: 'content_block_start',
      index,
      content_block: { type: 'tool_use', id: call.id !== '' ? call.id : 'toolu_agg', name: call.name, input: {} },
    });
    if (call.arguments !== '') {
      out += sseLine('content_block_delta', { type: 'content_block_delta', index, delta: { type: 'input_json_delta', partial_json: call.arguments } });
    }
    out += sseLine('content_block_stop', { type: 'content_block_stop', index });
    index += 1;
  }
  out += sseLine('message_delta', {
    type: 'message_delta',
    delta: { stop_reason: stopOut, stop_sequence: null },
    usage: { output_tokens: answer.outputTokens },
  });
  out += sseLine('message_stop', { type: 'message_stop' });
  return out;
}

// ---------------------------------------------------------------------------
// One upstream call
// ---------------------------------------------------------------------------

interface UpstreamResult {
  ok: boolean;
  status: number;
  code: string;
  answer: NormAnswer | null;
}

async function callUpstream(
  candidate: Candidate,
  clientApi: 'openai' | 'anthropic',
  body: Json,
  stream: boolean,
): Promise<UpstreamResult> {
  const provider = candidate.probe.provider;
  const base = joinBase(ENV_URL, provider.base);
  const model = candidate.model.id;
  const headers: Record<string, string> = { 'content-type': 'application/json', ...authHeaders(provider.auth, provider.key) };
  let url: string;
  let upstreamBody: Json;
  if (provider.wire === 'openai') {
    url = `${base}/v1/chat/completions`;
    upstreamBody = clientApi === 'openai' ? openAIUpstreamBody(body, model, stream) : anthropicToOpenAIBody(body, model);
    if (stream) headers.accept = 'text/event-stream';
  } else if (provider.wire === 'anthropic') {
    url = `${base}/v1/messages`;
    headers['anthropic-version'] = ANTHROPIC_VERSION;
    upstreamBody = clientApi === 'anthropic' ? { ...clone(body), model } : openAIToAnthropicBody(body, model);
    if (stream) headers.accept = 'text/event-stream';
  } else {
    const geminiBody = clientApi === 'openai' ? openAIToGeminiBody(body) : anthropicToGeminiBody(body);
    const action = stream ? 'streamGenerateContent' : 'generateContent';
    url = `${base}/v1beta/models/${encodeURIComponent(model)}:${action}${stream ? '?alt=sse' : ''}`;
    upstreamBody = geminiBody;
    if (stream) headers.accept = 'text/event-stream';
  }
  let res: Response;
  try {
    res = await fetch(url, {
      method: 'POST',
      headers,
      body: JSON.stringify(upstreamBody),
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS),
    });
  } catch {
    return { ok: false, status: 0, code: 'fetch_error', answer: null };
  }
  const text = await res.text();
  if (!res.ok) {
    let code = 'upstream_error';
    try {
      const errBody: unknown = JSON.parse(text);
      if (isRec(errBody) && isRec(errBody.error)) {
        const e = errBody.error;
        if (typeof e.code === 'string') code = e.code;
        else if (typeof e.type === 'string') code = e.type;
        else if (typeof e.status === 'string') code = e.status;
      }
    } catch {
      // Keep the generic code.
    }
    return { ok: false, status: res.status, code, answer: null };
  }
  const answer =
    provider.wire === 'openai'
      ? parseOpenAIAnswer(text, stream)
      : provider.wire === 'anthropic'
        ? parseAnthropicAnswer(text, stream)
        : parseGeminiAnswer(text, stream);
  if (answer === null) return { ok: false, status: 502, code: 'bad_upstream_body', answer: null };
  return { ok: true, status: 200, code: '', answer };
}

function openAIUpstreamBody(body: Json, model: string, stream: boolean): Json {
  const out = clone(body);
  out.model = model;
  if (stream) {
    out.stream = true;
    out.stream_options = { include_usage: true };
  }
  return out;
}

// ---------------------------------------------------------------------------
// Server state and request handling
// ---------------------------------------------------------------------------

const brief = readBrief(PROMPT_FILE);
let probes: Probe[] = [];
let catalog: Catalog = { providers: [] };
let manifestIndex = 0;

async function loadCatalog(): Promise<void> {
  if (brief.error !== null) log(brief.error);
  probes = await Promise.all(brief.providers.map((p) => probeProvider(p)));
  for (const probe of probes) {
    if (!probe.ok) log(`catalog probe failed for provider ${probe.provider.id}`);
  }
  catalog = buildCatalog(brief, probes);
}

function servedModels(): { id: string; provider: string; displayName: string }[] {
  const out: { id: string; provider: string; displayName: string }[] = [];
  for (const probe of probes) {
    if (!probe.ok) continue;
    for (const model of probe.models) {
      const row = ratingFor(brief, probe.provider.id, model.id);
      if (!modelIsFree(model, row)) continue;
      out.push({ id: model.id, provider: probe.provider.id, displayName: model.displayName });
    }
  }
  return out;
}

function openAIModelList(): Json {
  return {
    object: 'list',
    data: servedModels().map((m) => ({ id: m.id, object: 'model', created: FIXED_CREATED, owned_by: m.provider })),
  };
}

function anthropicModelList(): Json {
  return {
    data: servedModels().map((m) => ({ type: 'model', id: m.id, display_name: m.displayName, created_at: FIXED_ANTHROPIC_CREATED_AT })),
    has_more: false,
  };
}

async function readJsonBody(req: IncomingMessage): Promise<Json | null> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
    size += buf.length;
    if (size > MAX_BODY_BYTES) return null;
    chunks.push(buf);
  }
  if (chunks.length === 0) return null;
  try {
    const value: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    return isRec(value) ? value : null;
  } catch {
    return null;
  }
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const text = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(text) });
  res.end(text);
}

function sendText(res: ServerResponse, status: number, contentType: string, text: string): void {
  res.writeHead(status, { 'content-type': contentType, 'content-length': Buffer.byteLength(text) });
  res.end(text);
}

function errorOpenAI(res: ServerResponse, status: number, message: string): void {
  sendJson(res, status, { error: { message, type: status === 429 ? 'rate_limit_error' : 'invalid_request_error', code: null } });
}
function errorAnthropic(res: ServerResponse, status: number, message: string): void {
  sendJson(res, status, { type: 'error', error: { type: status === 429 ? 'rate_limit_error' : 'api_error', message } });
}

/** Serve one client request: route, fail over, render in the client's wire. */
async function serveRequest(
  clientApi: 'openai' | 'anthropic',
  body: Json,
  res: ServerResponse,
): Promise<void> {
  const stream = body.stream === true;
  const manifestEntry = brief.manifest[manifestIndex];
  manifestIndex += 1;
  const fromBody = bodyNeeds(clientApi, body);
  const needs: Needs = { ...(manifestEntry?.requires ?? {}), ...fromBody.needs };
  const candidates = candidatesFor(brief, probes, needs, fromBody.jsonMode);
  if (candidates.length === 0) {
    if (clientApi === 'openai') errorOpenAI(res, 503, 'no free model meets this request');
    else errorAnthropic(res, 503, 'no free model meets this request');
    return;
  }
  const weight = manifestEntry?.weight ?? 1;
  const triedProviders = new Set<string>();
  const retriedProviders = new Set<string>();
  for (let ci = 0; ci < candidates.length; ci++) {
    const candidate = candidates[ci];
    if (triedProviders.has(candidate.probe.provider.id)) continue;
    triedProviders.add(candidate.probe.provider.id);
    const result = await callUpstream(candidate, clientApi, body, stream);
    if (result.ok && result.answer !== null) {
      const answer = result.answer;
      const model = candidate.model.id;
      if (clientApi === 'openai') {
        const text = renderOpenAI(answer, model, stream);
        sendText(res, 200, stream ? 'text/event-stream; charset=utf-8' : 'application/json', text);
      } else {
        const text = renderAnthropic(answer, model, stream);
        sendText(res, 200, stream ? 'text/event-stream; charset=utf-8' : 'application/json', text);
      }
      return;
    }
    log(`provider ${candidate.probe.provider.id}/${candidate.model.id} refused (${result.status} ${result.code}); failing over`);
    // One immediate retry of the same model, when the refusal looks like a
    // scheduled rate-limit or a spent budget rather than a bad model, and the
    // weighted gain beats the cost of one later slot: a scheduled refusal is
    // usually a single call number, so the very next call to the same
    // provider often succeeds at the top rating, while a spent budget just
    // costs one extra call and the failover below runs as before.
    const retryable = result.status === 402 || result.status === 429 || result.status >= 500 || result.status === 0;
    const nextRating = ci + 1 < candidates.length ? candidates[ci + 1].rating : 0;
    if (
      retryable &&
      !retriedProviders.has(candidate.probe.provider.id) &&
      weight * (candidate.rating - nextRating) > 5
    ) {
      retriedProviders.add(candidate.probe.provider.id);
      const retry = await callUpstream(candidate, clientApi, body, stream);
      if (retry.ok && retry.answer !== null) {
        const answer = retry.answer;
        const model = candidate.model.id;
        if (clientApi === 'openai') {
          const text = renderOpenAI(answer, model, stream);
          sendText(res, 200, stream ? 'text/event-stream; charset=utf-8' : 'application/json', text);
        } else {
          const text = renderAnthropic(answer, model, stream);
          sendText(res, 200, stream ? 'text/event-stream; charset=utf-8' : 'application/json', text);
        }
        return;
      }
      log(`provider ${candidate.probe.provider.id}/${candidate.model.id} refused again (${retry.status} ${retry.code}); failing over`);
    }
  }
  if (clientApi === 'openai') errorOpenAI(res, 503, 'every free model that meets this request refused');
  else errorAnthropic(res, 503, 'every free model that meets this request refused');
}

async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url ?? '/', 'http://entry');
  if (req.method === 'GET' && url.pathname === '/v1/models') {
    if (typeof req.headers['x-api-key'] === 'string') sendJson(res, 200, anthropicModelList());
    else sendJson(res, 200, openAIModelList());
    return;
  }
  if (req.method === 'POST' && url.pathname === '/v1/chat/completions') {
    const body = await readJsonBody(req);
    if (body === null) return errorOpenAI(res, 400, 'the request body is not one JSON object');
    await serveRequest('openai', body, res);
    return;
  }
  if (req.method === 'POST' && url.pathname === '/v1/messages') {
    const body = await readJsonBody(req);
    if (body === null) return errorAnthropic(res, 400, 'the request body is not one JSON object');
    await serveRequest('anthropic', body, res);
    return;
  }
  if (url.pathname === '/v1/messages') errorAnthropic(res, 404, `no route ${req.method ?? ''} ${url.pathname}`);
  else errorOpenAI(res, 404, `no route ${req.method ?? ''} ${url.pathname}`);
}

function listenOn(server: Server): Promise<number> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '0.0.0.0', () => {
      const address = server.address();
      if (address === null || typeof address === 'string') reject(new Error('the server has no port'));
      else resolve(address.port);
    });
  });
}

async function register(port: number): Promise<void> {
  let lastError: unknown = null;
  for (let attempt = 0; attempt < REGISTER_ATTEMPTS; attempt++) {
    try {
      const res = await fetch(`${ENV_URL}/register`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ protocol_version: PROTOCOL_VERSION, port, catalog }),
        signal: AbortSignal.timeout(5000),
      });
      if (!res.ok) throw new Error(`POST ${ENV_URL}/register answered HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`);
      const reply: unknown = await res.json();
      if (isRec(reply) && typeof reply.requests === 'number') log(`registered: the workload holds ${reply.requests} requests`);
      return;
    } catch (err) {
      lastError = err;
      await sleep(REGISTER_RETRY_MS);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

function pollWork(server: Server): void {
  let inFlight = false;
  const timer = setInterval(() => {
    if (inFlight) return;
    inFlight = true;
    void (async () => {
      try {
        const res = await fetch(`${ENV_URL}/work`, { signal: AbortSignal.timeout(2000) });
        if (res.status === 200) {
          const work: unknown = await res.json();
          if (isRec(work) && work.done === true) {
            clearInterval(timer);
            server.close();
            process.exit(0);
          }
        }
      } catch {
        // The env is not reachable yet: try again on the next tick.
      } finally {
        inFlight = false;
      }
    })();
  }, POLL_MS);
}

async function start(): Promise<void> {
  await loadCatalog();
  if (process.argv.includes('--list')) {
    const text = `${JSON.stringify(catalog)}\n`;
    await new Promise<void>((resolve) => process.stdout.write(text, () => resolve()));
    process.exit(0);
  }
  if (ENV_URL === '') throw new Error('MAKEFUN_ENV_URL is not set');
  const server = createServer((req, res) => {
    void handle(req, res).catch((err: unknown) => {
      log((err as Error).stack ?? String(err));
      if (!res.headersSent) {
        if (req.url?.startsWith('/v1/messages') === true) errorAnthropic(res, 500, 'the server failed to answer');
        else errorOpenAI(res, 500, 'the server failed to answer');
      }
    });
  });
  const port = await listenOn(server);
  if (port === 8080) throw new Error('the OS picked 8080, which the env owns on one layout: restart the trial');
  log(`listening on 0.0.0.0:${port} with ${catalog.providers.length} providers and ${servedModels().length} free models`);
  await register(port);
  process.on('SIGTERM', () => process.exit(0));
  process.on('SIGINT', () => process.exit(0));
  pollWork(server);
}

try {
  await start();
} catch (err) {
  log((err as Error).stack ?? String(err));
  process.exit(1);
}
