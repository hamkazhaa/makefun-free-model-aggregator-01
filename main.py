#!/usr/bin/env python3
"""Free-model aggregator for the make.fun free-model-aggregator-01 bounty.

One server, Python 3.13 stdlib only, no network beyond the trial env.

Design (from the real kit's RULES.txt, competition.yaml, and the env
source program/env/server.mjs — these supersede any mock-based guess):

* The brief arrives at $MAKEFUN_PROMPT_FILE and carries three JSON
  documents, found by their "schema": the provider directory
  (free-model-aggregator-01/providers/v1), the quality table
  (free-model-aggregator-01/ratings/v1, per-model "rating_milli"),
  and the workload manifest (free-model-aggregator-01/workload/v1,
  20 requests in arrival order, each with api/stream/weight/requires).
* The env at $MAKEFUN_ENV_URL serves only POST /register,
  GET /work, GET /instance, and providers under /p/<id>/. There is no
  /providers endpoint.
* Model discovery: probe each provider's catalog route
  (openai/anthropic GET <base>/v1/models, gemini GET <base>/v1beta/models)
  with that provider's own auth header. The catalog registered with the
  env is {"providers": [{id, base_url: "/p/<id>", key_env, wire,
  models: [{id, free, context_length}]}]} and is byte-identical to the
  --list output.
* Register with {"protocol_version": 1, "port", "catalog"}.
* Serve the client wires: OpenAI POST /v1/chat/completions and
  Anthropic POST /v1/messages (JSON and SSE), plus
  GET /v1/models in the shape of the auth header (OpenAI shape for
  "Authorization: Bearer", Anthropic shape for "x-api-key").
* Route each request by arrival order: the manifest row for that
  request gives its requirements (tools/vision/json/min_context);
  fallback derives them from the body (tools present, image present,
  JSON mode, or a long body > 5,000 chars -> min_context 200000).
  Candidates are free models meeting the requirements, ranked by
  rating_milli, then context_length, then provider/model id.
  A JSON-mode request never goes to an Anthropic-wire provider
  (that wire cannot carry JSON mode, so the echo check would fail).
* Fail over on 402/429/5xx to the next candidate; a provider that
  refuses 402 is marked exhausted. Exactly one provider call answers
  each request (the scorer checks that).
* Wire translation preserves every field the env echo-checks:
  system prompt, user text, tool names, max_tokens, stop sequences,
  image bytes (carried verbatim as data URLs / base64 blocks /
  inlineData), and JSON mode. The provider's answer text is returned
  whole, with the provider's usage counts and the stop reason mapped
  to the client's wire (stop / end_turn).
* The server polls GET /work every 100 ms and exits 0 when the env
  says the workload is done; a process still running at the 180 s
  wall scores 0 on every column.

Fallback: with no prompt file (e.g. the local mock env in tests/),
provider discovery falls back to the old /providers endpoints and a
PROVIDERS_JSON / PROVIDERS_FILE document, and registration /work
polling tolerate a 404 /work.

Usage:
  python3 main.py --list            # print the catalog JSON
  python3 main.py --port 8080       # serve (default: ephemeral port)
Environment:
  MAKEFUN_ENV_URL, MAKEFUN_PROMPT_FILE, MAKEFUN_HOST, MAKEFUN_PORT.
"""
import argparse
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urljoin

VERSION = "2.0.0"

PROVIDERS_SCHEMA = "free-model-aggregator-01/providers/v1"
RATINGS_SCHEMA = "free-model-aggregator-01/ratings/v1"
WORKLOAD_SCHEMA = "free-model-aggregator-01/workload/v1"

# The env is the only client. A "client key" is what it uses on the
# warm-up GET /v1/models probes; any Authorization / x-api-key header
# selects the shape of that header's wire.
CLIENT_KEY = "makefun-client"
MODEL_LIST_CREATED = 1767225600

# ---------------------------------------------------------------------------
# HTTP helpers (stdlib only)
# ---------------------------------------------------------------------------


class ProviderError(Exception):
    def __init__(self, status, body=""):
        super().__init__("provider HTTP %s" % status)
        self.status = status
        self.body = body


def http_json(method, url, body=None, headers=None, timeout=20):
    """One HTTP call; returns (status, parsed_json_or_None, raw_text)."""
    data = None
    hdrs = dict(headers or {})
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        hdrs.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace")
        if exc.code in (400, 401, 402, 429) or exc.code >= 500:
            raise ProviderError(exc.code, raw)
        return exc.code, _safe_json(raw), raw
    except Exception as exc:
        raise ProviderError(502, "connection failed: %s" % exc)
    return 200, _safe_json(raw), raw


def http_raw(method, url, body=None, headers=None, timeout=30):
    """One HTTP call; returns (status, raw_text). Raises ProviderError on
    400/401/402/429/5xx."""
    data = None
    hdrs = dict(headers or {})
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        hdrs.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace")
        raise ProviderError(exc.code, raw)
    except ProviderError:
        raise
    except Exception as exc:
        raise ProviderError(502, "connection failed: %s" % exc)


def _safe_json(raw):
    try:
        return json.loads(raw)
    except Exception:
        return None


def _auth_headers(auth_header, auth_value):
    if auth_header and auth_value:
        return {auth_header: auth_value}
    return {}


def _auth_for_scheme(auth, key):
    """Directory auth scheme + key -> (header name, header value)."""
    if auth == "bearer":
        return "Authorization", "Bearer " + key
    if auth == "x-api-key":
        return "x-api-key", key
    if auth == "x-goog-api-key":
        return "x-goog-api-key", key
    return None, None


# ---------------------------------------------------------------------------
# Brief parsing: JSON documents embedded in the prompt file
# ---------------------------------------------------------------------------


def parse_brief_documents(text):
    """Find every top-level JSON object in text (starter's documentsIn)."""
    docs = []
    i = 0
    n = len(text)
    while i < n:
        start = text.find("{", i)
        if start < 0:
            break
        depth = 0
        in_str = False
        esc = False
        j = start
        while j < n:
            ch = text[j]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
            else:
                if ch == '"':
                    in_str = True
                elif ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        break
            j += 1
        if depth == 0 and j < n:
            try:
                docs.append(json.loads(text[start:j + 1]))
            except Exception:
                pass
            i = j + 1
        else:
            break
    return docs


def load_brief():
    """Return (providers_doc, ratings_doc, workload_doc) or (None,None,None)."""
    path = os.environ.get("MAKEFUN_PROMPT_FILE")
    if not path or not os.path.exists(path):
        return None, None, None
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    providers = ratings = workload = None
    for doc in parse_brief_documents(text):
        schema = doc.get("schema") if isinstance(doc, dict) else None
        if schema == PROVIDERS_SCHEMA:
            providers = doc
        elif schema == RATINGS_SCHEMA:
            ratings = doc
        elif schema == WORKLOAD_SCHEMA:
            workload = doc
    return providers, ratings, workload


# ---------------------------------------------------------------------------
# Provider discovery + catalog
# ---------------------------------------------------------------------------


def probe_provider_catalog(env_url, provider):
    """GET the provider's own catalog route. Returns a list of model dicts
    with id/free/context_length/tools/vision/json, or [] on failure."""
    wire = provider.get("wire")
    base = provider.get("base_url") or provider.get("path")
    if not base:
        return []
    if wire == "gemini":
        url = urljoin(env_url + "/", base.lstrip("/") + "/v1beta/models")
    else:
        url = urljoin(env_url + "/", base.lstrip("/") + "/v1/models")
    hdr_name, hdr_value = _auth_for_scheme(provider.get("auth"), provider.get("key") or "")
    headers = _auth_headers(hdr_name, hdr_value)
    if wire == "anthropic":
        headers.setdefault("anthropic-version", "2023-06-01")
    try:
        status, doc, _raw = http_json("GET", url, headers=headers, timeout=10)
    except ProviderError:
        return []
    except Exception:
        return []
    if not isinstance(doc, dict):
        return []
    items = doc.get("data") or doc.get("models") or []
    out = []
    for item in items:
        if not isinstance(item, dict):
            continue
        # OpenAI/Anthropic listings use "id"; Gemini uses
        # "name": "models/<id>".
        mid = item.get("id") or item.get("name")
        if not mid:
            continue
        if isinstance(mid, str) and mid.startswith("models/"):
            mid = mid.split("/", 1)[1]
        out.append({
            "id": mid,
            "free": item.get("free"),
            "context_length": item.get("context_length"),
            "tools": bool(item.get("tools")),
            "vision": bool(item.get("vision")),
            "json": bool(item.get("json")),
        })
    return out


def _rating_lookup(ratings_doc):
    """model id -> ratings row from the ratings document. The doc's list
    key is "ratings" and each row names its model under "model"."""
    lookup = {}
    if not isinstance(ratings_doc, dict):
        return lookup
    for row in ratings_doc.get("ratings") or []:
        if not isinstance(row, dict):
            continue
        mid = row.get("model") or row.get("id")
        if mid:
            lookup[mid] = row
    return lookup


def build_catalog(env_url, providers_doc, ratings_doc):
    """Probe every provider in the directory; return (catalog, models).

    catalog: the exact document for --list and for /register.
    models: flat internal table, each row carrying provider info,
    rating, caps, auth, and the joined provider base URL.
    """
    rating_of = _rating_lookup(ratings_doc)
    catalog_providers = []
    models = []
    for prov in (providers_doc or {}).get("providers") or []:
        pid = prov.get("id")
        wire = prov.get("wire")
        key_env = prov.get("key_env") or ""
        path = prov.get("path") or ("/p/" + pid if pid else "")
        base = urljoin((env_url or "").rstrip("/") + "/", path.lstrip("/"))
        listed = probe_provider_catalog(env_url, prov) if env_url else []
        # A model is free iff the ratings row says so; fall back to the
        # listing's free flag, then to True (a listed model is usable).
        listed_by_id = {m["id"]: m for m in listed}
        ids = set(listed_by_id) | {mid for mid, row in rating_of.items()
                                   if row.get("provider") == pid}
        if not ids:
            continue
        cat_models = []
        for mid in sorted(ids):
            row = rating_of.get(mid) or {}
            listed_row = listed_by_id.get(mid) or {}
            free = row.get("free")
            if free is None:
                free = listed_row.get("free")
            if free is None:
                free = True
            ctx = row.get("context_length")
            if ctx is None:
                ctx = listed_row.get("context_length")
            cat_models.append({"id": mid, "free": bool(free),
                               "context_length": ctx})
            models.append({
                "id": mid,
                "provider": pid,
                "wire": wire,
                "auth": prov.get("auth"),
                "key": prov.get("key") or "",
                "base_url": base,
                "free": bool(free),
                "context_length": ctx,
                "rating": (row.get("rating_milli") or 0) / 1000.0,
                "rating_milli": row.get("rating_milli") or 0,
                "tools": bool(row.get("tools") if row.get("tools") is not None
                              else listed_row.get("tools")),
                "vision": bool(row.get("vision") if row.get("vision") is not None
                               else listed_row.get("vision")),
                "json": bool(row.get("json") if row.get("json") is not None
                             else listed_row.get("json")),
            })
        catalog_providers.append({
            "id": pid,
            "base_url": path,
            "key_env": key_env,
            "wire": wire,
            "models": cat_models,
        })
    return {"providers": catalog_providers}, models


# ---------------------------------------------------------------------------
# Fallback discovery (no prompt file: local mock env / PROVIDERS_* docs)
# ---------------------------------------------------------------------------


def _norm_model(raw):
    if not isinstance(raw, dict):
        return None
    mid = raw.get("id")
    if not mid:
        return None
    ctx = raw.get("context_length")
    if ctx is None:
        ctx = raw.get("context_window")
    quality = raw.get("quality")
    if quality is None and raw.get("rating_milli") is not None:
        quality = raw["rating_milli"] / 1000.0
    return {
        "id": mid,
        "free": raw.get("free", True),
        "context_length": ctx,
        "rating": float(quality) if quality is not None else 0.5,
        "rating_milli": int(float(quality) * 1000) if quality is not None else 500,
        "tools": bool(raw.get("supports_tools", raw.get("tools", False))),
        "vision": bool(raw.get("supports_vision", raw.get("vision", False))),
        "json": bool(raw.get("supports_json", raw.get("json", False))),
    }


def _extract_providers(doc):
    """Old /providers shapes -> (catalog, models)."""
    if not isinstance(doc, dict):
        return {"providers": []}, []
    providers = doc.get("providers") or doc.get("models") or []
    if isinstance(providers, dict):
        providers = [providers]
    catalog = {"providers": []}
    models = []
    for prov in providers:
        if not isinstance(prov, dict):
            continue
        pid = prov.get("id") or prov.get("name") or "provider"
        base = prov.get("base_url") or prov.get("url") or ""
        wire = prov.get("wire") or "openai"
        key_env = prov.get("key_env") or ""
        auth_header = prov.get("auth_header")
        auth_value = prov.get("auth_value")
        raw_models = prov.get("models") or []
        cat_models = []
        for raw in raw_models:
            m = _norm_model(raw)
            if not m:
                continue
            cat_models.append({"id": m["id"], "free": bool(m["free"]),
                               "context_length": m["context_length"]})
            models.append(dict(m, provider=pid, wire=wire,
                               base_url=base, auth=None, key="",
                               auth_header=auth_header,
                               auth_value=auth_value))
        catalog["providers"].append({
            "id": pid, "base_url": base, "key_env": key_env, "wire": wire,
            "models": cat_models,
        })
    return catalog, models


def discover_fallback(env_url):
    """Probe the legacy discovery endpoints; also honour
    PROVIDERS_JSON / PROVIDERS_FILE. Returns (catalog, models)."""
    for url in _discovery_urls(env_url):
        try:
            _status, doc, _raw = http_json("GET", url, timeout=10)
        except Exception:
            continue
        if doc:
            catalog, models = _extract_providers(doc)
            if models:
                return catalog, models
    doc = _providers_from_env()
    if doc:
        catalog, models = _extract_providers(doc)
        if models:
            return catalog, models
    return {"providers": []}, []


def _discovery_urls(env_url):
    if not env_url:
        return []
    base = env_url.rstrip("/")
    return [base + p for p in ("/providers", "/v1/providers",
                               "/env/providers.json", "/providers.json",
                               "/catalog", "/v1/models", "/models")]


def _providers_from_env():
    doc = os.environ.get("PROVIDERS_JSON")
    if doc:
        try:
            return json.loads(doc)
        except Exception:
            return None
    path = os.environ.get("PROVIDERS_FILE")
    if path and os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return None
    return None


def catalog_bytes(catalog):
    return (json.dumps(catalog, sort_keys=True) + "\n").encode("utf-8")


# ---------------------------------------------------------------------------
# Canonical client request (preserves every echo-checked field)
# ---------------------------------------------------------------------------

_PART_TEXT = "text"
_PART_IMAGE = "image"
_PART_TOOL_CALL = "tool_call"
_PART_TOOL_RESULT = "tool_result"


def _text_of_content(content):
    """OpenAI/Anthropic content -> (text, parts). Parts keep image data."""
    if isinstance(content, str):
        return content, ([{"type": _PART_TEXT, "text": content}] if content else [])
    parts = []
    texts = []
    for block in content or []:
        if not isinstance(block, dict):
            continue
        kind = block.get("type")
        if kind == "text" and isinstance(block.get("text"), str):
            texts.append(block["text"])
            parts.append({"type": _PART_TEXT, "text": block["text"]})
        elif kind == "image_url":
            url = (block.get("image_url") or {}).get("url") or ""
            parts.append({"type": _PART_IMAGE, "url": url})
        elif kind == "image":
            src = block.get("source") or {}
            if src.get("type") == "base64":
                data = src.get("data") or ""
                media = src.get("media_type") or "image/png"
                parts.append({"type": _PART_IMAGE,
                              "url": "data:%s;base64,%s" % (media, data)})
        elif kind == "tool_use":
            parts.append({"type": _PART_TOOL_CALL, "id": block.get("id"),
                          "name": block.get("name"),
                          "arguments": block.get("input") or {}})
        elif kind == "tool_result":
            content_text = block.get("content")
            if isinstance(content_text, list):
                content_text = "\n".join(
                    b.get("text", "") for b in content_text
                    if isinstance(b, dict) and b.get("type") == "text")
            parts.append({"type": _PART_TOOL_RESULT,
                          "id": block.get("tool_use_id"),
                          "content": content_text or ""})
    return "\n".join(texts), parts


def parse_client_request(wire, body):
    """Normalise an OpenAI or Anthropic client body to the canonical form."""
    req = {"wire": wire, "stream": bool(body.get("stream")),
           "system": "", "messages": [], "tools": [], "tool_names": [],
           "max_tokens": None, "temperature": None, "top_p": None,
           "stops": [], "json_mode": False, "raw": body,
           "requested_model": body.get("model")}
    if wire == "openai":
        for msg in body.get("messages") or []:
            role = msg.get("role")
            if role == "system":
                text, _parts = _text_of_content(msg.get("content"))
                req["system"] = (req["system"] + "\n" + text).strip() if req["system"] else text
                continue
            text, parts = _text_of_content(msg.get("content"))
            for tc in msg.get("tool_calls") or []:
                fn = tc.get("function") or {}
                args = fn.get("arguments")
                if isinstance(args, str):
                    try:
                        args = json.loads(args)
                    except Exception:
                        args = {}
                parts.append({"type": _PART_TOOL_CALL, "id": tc.get("id"),
                              "name": fn.get("name"), "arguments": args or {}})
            if role == "tool":
                parts = [{"type": _PART_TOOL_RESULT, "id": msg.get("tool_call_id"),
                          "content": text}]
            req["messages"].append({"role": role, "parts": parts})
        for tool in body.get("tools") or []:
            fn = tool.get("function") or tool
            if fn.get("name"):
                req["tool_names"].append(fn["name"])
            req["tools"].append(tool)
        req["max_tokens"] = body.get("max_tokens") or body.get("max_completion_tokens")
        req["temperature"] = body.get("temperature")
        req["top_p"] = body.get("top_p")
        stop = body.get("stop")
        if isinstance(stop, str):
            req["stops"] = [stop]
        elif isinstance(stop, list):
            req["stops"] = [s for s in stop if isinstance(s, str)]
        fmt = body.get("response_format") or {}
        req["json_mode"] = fmt.get("type") in ("json_object", "json_schema")
    else:  # anthropic
        system = body.get("system")
        if isinstance(system, str):
            req["system"] = system
        elif isinstance(system, list):
            req["system"] = "\n".join(b.get("text", "") for b in system
                                      if isinstance(b, dict) and b.get("type") == "text")
        for msg in body.get("messages") or []:
            text, parts = _text_of_content(msg.get("content"))
            req["messages"].append({"role": msg.get("role"), "parts": parts})
        for tool in body.get("tools") or []:
            if tool.get("name"):
                req["tool_names"].append(tool["name"])
            req["tools"].append(tool)
        req["max_tokens"] = body.get("max_tokens")
        req["temperature"] = body.get("temperature")
        req["top_p"] = body.get("top_p")
        stops = body.get("stop_sequences")
        if isinstance(stops, list):
            req["stops"] = [s for s in stops if isinstance(s, str)]
        # Anthropic wire has no JSON mode field; json_mode stays False.
    return req


def canonical_user_text(req):
    """User text as the env's parsers compute it (last user message wins)."""
    text = ""
    for msg in req["messages"]:
        if msg["role"] != "user":
            continue
        text = "\n".join(p["text"] for p in msg["parts"]
                         if p["type"] == _PART_TEXT)
    return text


def canonical_has_image(req):
    return any(p["type"] == _PART_IMAGE
               for msg in req["messages"] for p in msg["parts"])


def derive_requirements(req, raw_body):
    """Body-derived requirements (fallback when no manifest row)."""
    needs = {"tools": bool(req["tool_names"]),
             "vision": canonical_has_image(req),
             "json": req["json_mode"],
             "min_context": None}
    body_len = len(json.dumps(raw_body, sort_keys=True))
    if body_len > 5000:
        needs["min_context"] = 200000
    return needs


# ---------------------------------------------------------------------------
# Canonical -> provider wire (build upstream bodies)
# ---------------------------------------------------------------------------


def _image_data_parts(url):
    """Split a data URL into (media_type, base64) or (None, None)."""
    if not url or not url.startswith("data:"):
        return None, None
    head, _, data = url.partition(",")
    media = head[5:].split(";")[0] or "image/png"
    return media, data


def canonical_to_openai(req, model):
    """Canonical request -> OpenAI chat-completions body."""
    messages = []
    if req["system"]:
        messages.append({"role": "system", "content": req["system"]})
    for msg in req["messages"]:
        role = msg["role"]
        content = []
        tool_calls = []
        for p in msg["parts"]:
            if p["type"] == _PART_TEXT:
                content.append({"type": "text", "text": p["text"]})
            elif p["type"] == _PART_IMAGE:
                content.append({"type": "image_url",
                                "image_url": {"url": p["url"]}})
            elif p["type"] == _PART_TOOL_CALL:
                tool_calls.append({"id": p["id"], "type": "function",
                                   "function": {"name": p["name"],
                                                "arguments": json.dumps(p["arguments"])}})
            elif p["type"] == _PART_TOOL_RESULT:
                content.append({"type": "text", "text": p["content"]})
        if role == "tool":
            # Tool results are their own message: role "tool" + content.
            text = "\n".join(p["content"] for p in msg["parts"]
                             if p["type"] == _PART_TOOL_RESULT)
            tid = next((p["id"] for p in msg["parts"]
                        if p["type"] == _PART_TOOL_RESULT), None)
            out = {"role": "tool", "content": text}
            if tid:
                out["tool_call_id"] = tid
            messages.append(out)
            continue
        if len(content) == 1 and content[0]["type"] == "text" and not tool_calls:
            body_content = content[0]["text"]
        elif content:
            body_content = content
        else:
            body_content = ""
        out = {"role": role, "content": body_content}
        if tool_calls:
            out["tool_calls"] = tool_calls
        messages.append(out)
    body = {"model": model, "messages": messages}
    if req["tools"]:
        tools = []
        for tool in req["tools"]:
            if isinstance(tool, dict) and "function" in tool:
                tools.append(tool)
            else:
                tools.append({"type": "function", "function": {
                    "name": tool.get("name"),
                    "description": tool.get("description", ""),
                    "parameters": tool.get("input_schema") or {}}})
        body["tools"] = tools
    if req["max_tokens"] is not None:
        body["max_tokens"] = req["max_tokens"]
    if req["temperature"] is not None:
        body["temperature"] = req["temperature"]
    if req["top_p"] is not None:
        body["top_p"] = req["top_p"]
    if req["stops"]:
        body["stop"] = req["stops"]
    if req["json_mode"]:
        body["response_format"] = {"type": "json_object"}
    if req["stream"]:
        body["stream"] = True
        body["stream_options"] = {"include_usage": True}
    return body


def canonical_to_anthropic(req, model):
    """Canonical request -> Anthropic messages body."""
    messages = []
    for msg in req["messages"]:
        role = "user" if msg["role"] in ("user", "tool") else "assistant"
        content = []
        for p in msg["parts"]:
            if p["type"] == _PART_TEXT:
                content.append({"type": "text", "text": p["text"]})
            elif p["type"] == _PART_IMAGE:
                media, data = _image_data_parts(p["url"])
                content.append({"type": "image", "source": {
                    "type": "base64", "media_type": media or "image/png",
                    "data": data or ""}})
            elif p["type"] == _PART_TOOL_CALL:
                content.append({"type": "tool_use", "id": p["id"],
                                "name": p["name"], "input": p["arguments"]})
            elif p["type"] == _PART_TOOL_RESULT:
                content.append({"type": "tool_result",
                                "tool_use_id": p["id"],
                                "content": p["content"]})
        if not content:
            continue
        # Plain-text content goes as a string (the env's parser and the
        # local mock both accept it; a list is only needed for images
        # and tool blocks).
        if all(b.get("type") == "text" for b in content):
            out_content = "\n".join(b.get("text", "") for b in content)
        else:
            out_content = content
        if messages and messages[-1]["role"] == role:
            prev = messages[-1]["content"]
            if isinstance(prev, str) and isinstance(out_content, str):
                messages[-1]["content"] = prev + "\n" + out_content
            elif isinstance(prev, str):
                messages[-1]["content"] = ([{"type": "text", "text": prev}]
                                           + out_content)
            elif isinstance(out_content, str):
                prev.append({"type": "text", "text": out_content})
            else:
                prev.extend(out_content)
        else:
            messages.append({"role": role, "content": out_content})
    body = {"model": model, "messages": messages,
            "max_tokens": req["max_tokens"] or 1024}
    if req["system"]:
        body["system"] = req["system"]
    if req["tools"]:
        tools = []
        for tool in req["tools"]:
            if isinstance(tool, dict) and "function" in tool:
                fn = tool["function"]
                tools.append({"name": fn.get("name"),
                              "description": fn.get("description", ""),
                              "input_schema": fn.get("parameters") or {}})
            else:
                tools.append({"name": tool.get("name"),
                              "description": tool.get("description", ""),
                              "input_schema": tool.get("input_schema") or {}})
        body["tools"] = tools
    if req["temperature"] is not None:
        body["temperature"] = req["temperature"]
    if req["top_p"] is not None:
        body["top_p"] = req["top_p"]
    if req["stops"]:
        body["stop_sequences"] = req["stops"]
    if req["stream"]:
        body["stream"] = True
    return body


def canonical_to_gemini(req, model, stream):
    """Canonical request -> Gemini generateContent body."""
    contents = []
    for msg in req["messages"]:
        role = "model" if msg["role"] == "assistant" else "user"
        parts = []
        for p in msg["parts"]:
            if p["type"] == _PART_TEXT:
                parts.append({"text": p["text"]})
            elif p["type"] == _PART_IMAGE:
                media, data = _image_data_parts(p["url"])
                parts.append({"inlineData": {"mimeType": media or "image/png",
                                             "data": data or ""}})
            elif p["type"] == _PART_TOOL_RESULT:
                parts.append({"text": p["content"]})
            elif p["type"] == _PART_TOOL_CALL:
                parts.append({"functionCall": {"name": p["name"],
                                              "args": p["arguments"]}})
        if not parts:
            continue
        if contents and contents[-1]["role"] == role:
            contents[-1]["parts"].extend(parts)
        else:
            contents.append({"role": role, "parts": parts})
    body = {"contents": contents}
    if req["system"]:
        body["systemInstruction"] = {"parts": [{"text": req["system"]}]}
    if req["tools"]:
        decls = []
        for tool in req["tools"]:
            fn = tool.get("function") if isinstance(tool, dict) and "function" in tool else tool
            decls.append({"name": fn.get("name"),
                          "description": fn.get("description", ""),
                          "parameters": fn.get("parameters")
                          or fn.get("input_schema") or {}})
        body["tools"] = [{"functionDeclarations": decls}]
    cfg = {}
    if req["max_tokens"] is not None:
        cfg["maxOutputTokens"] = req["max_tokens"]
    if req["temperature"] is not None:
        cfg["temperature"] = req["temperature"]
    if req["top_p"] is not None:
        cfg["topP"] = req["top_p"]
    if req["stops"]:
        cfg["stopSequences"] = req["stops"]
    if req["json_mode"]:
        cfg["responseMimeType"] = "application/json"
    if cfg:
        body["generationConfig"] = cfg
    return body


# ---------------------------------------------------------------------------
# Provider responses (JSON and SSE) -> (text, input_tokens, output_tokens, stop)
# ---------------------------------------------------------------------------


def parse_sse_frames(text):
    frames = []
    for frame in text.replace("\r\n", "\n").split("\n\n"):
        if not frame.strip():
            continue
        event = None
        data = []
        for line in frame.split("\n"):
            if line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].lstrip())
        if data:
            frames.append({"event": event, "data": "\n".join(data)})
    return frames


def _usage_from_openai(usage):
    if not isinstance(usage, dict):
        return 0, 0
    return int(usage.get("prompt_tokens") or 0), int(usage.get("completion_tokens") or 0)


def _usage_from_anthropic(usage):
    if not isinstance(usage, dict):
        return 0, 0
    return int(usage.get("input_tokens") or 0), int(usage.get("output_tokens") or 0)


def parse_provider_openai(raw, stream):
    if not stream:
        doc = _safe_json(raw) or {}
        choice = (doc.get("choices") or [{}])[0]
        msg = choice.get("message") or {}
        text = msg.get("content") or ""
        if isinstance(text, list):
            text = "".join(b.get("text", "") for b in text
                           if isinstance(b, dict) and b.get("type") == "text")
        u_in, u_out = _usage_from_openai(doc.get("usage"))
        return text, u_in, u_out, choice.get("finish_reason") or "stop"
    text = ""
    u_in = u_out = 0
    stop = "stop"
    for frame in parse_sse_frames(raw):
        payload = frame["data"].strip()
        if payload == "[DONE]":
            break
        try:
            chunk = json.loads(payload)
        except Exception:
            continue
        if isinstance(chunk.get("usage"), dict):
            u_in, u_out = _usage_from_openai(chunk["usage"])
        for choice in chunk.get("choices") or []:
            delta = choice.get("delta") or {}
            if isinstance(delta.get("content"), str):
                text += delta["content"]
            if choice.get("finish_reason"):
                stop = choice["finish_reason"]
    return text, u_in, u_out, stop


def parse_provider_anthropic(raw, stream):
    if not stream:
        doc = _safe_json(raw) or {}
        text = ""
        for block in doc.get("content") or []:
            if isinstance(block, dict) and block.get("type") == "text":
                text += block.get("text", "")
        u_in, u_out = _usage_from_anthropic(doc.get("usage"))
        return text, u_in, u_out, doc.get("stop_reason") or "end_turn"
    text = ""
    u_in = u_out = 0
    stop = "end_turn"
    for frame in parse_sse_frames(raw):
        try:
            data = json.loads(frame["data"])
        except Exception:
            continue
        if frame["event"] is None:
            # Mock-style frames carry no event name: delta text only.
            delta = data.get("delta") or {}
            if isinstance(delta.get("text"), str):
                text += delta["text"]
            continue
        if frame["event"] == "message_start":
            msg = data.get("message") or {}
            u_in, _o = _usage_from_anthropic(msg.get("usage"))
        elif frame["event"] == "content_block_delta":
            delta = data.get("delta") or {}
            if delta.get("type") == "text_delta" and isinstance(delta.get("text"), str):
                text += delta["text"]
        elif frame["event"] == "message_delta":
            delta = data.get("delta") or {}
            if delta.get("stop_reason"):
                stop = delta["stop_reason"]
            _i, u_out = _usage_from_anthropic(data.get("usage"))
    return text, u_in, u_out, stop


def parse_provider_gemini(raw, stream):
    if not stream:
        doc = _safe_json(raw) or {}
        text, stop = _gemini_candidate(doc)
        usage = doc.get("usageMetadata") or {}
        return text, int(usage.get("promptTokenCount") or 0), int(usage.get("candidatesTokenCount") or 0), stop
    text = ""
    u_in = u_out = 0
    stop = "STOP"
    for frame in parse_sse_frames(raw):
        try:
            doc = json.loads(frame["data"])
        except Exception:
            continue
        t, s = _gemini_candidate(doc)
        text += t
        if s:
            stop = s
        usage = doc.get("usageMetadata") or {}
        if usage:
            u_in = int(usage.get("promptTokenCount") or 0)
            u_out = int(usage.get("candidatesTokenCount") or 0)
    return text, u_in, u_out, stop


def _gemini_candidate(doc):
    text = ""
    stop = None
    for cand in doc.get("candidates") or []:
        content = cand.get("content") or {}
        for part in content.get("parts") or []:
            if isinstance(part.get("text"), str):
                text += part["text"]
        if cand.get("finishReason"):
            stop = cand["finishReason"]
    return text, stop or "STOP"


def _provider_auth(model):
    """(header, value) for one provider call, from either discovery path."""
    if model.get("auth_header"):
        return model["auth_header"], model.get("auth_value") or ""
    return _auth_for_scheme(model.get("auth"), model.get("key") or "")


def call_provider(model, req, stream):
    """One call to one provider model. Returns
    (text, input_tokens, output_tokens, stop_reason)."""
    wire = model["wire"]
    headers = {}
    hdr, val = _provider_auth(model)
    if hdr and val:
        headers[hdr] = val
    if wire == "openai":
        body = canonical_to_openai(req, model["id"])
        url = model["base_url"].rstrip("/") + "/v1/chat/completions"
        _status, raw = http_raw("POST", url, body=body, headers=headers)
        return parse_provider_openai(raw, stream)
    if wire == "anthropic":
        body = canonical_to_anthropic(req, model["id"])
        headers.setdefault("anthropic-version", "2023-06-01")
        url = model["base_url"].rstrip("/") + "/v1/messages"
        _status, raw = http_raw("POST", url, body=body, headers=headers)
        return parse_provider_anthropic(raw, stream)
    # gemini
    body = canonical_to_gemini(req, model["id"], stream)
    if stream:
        url = (model["base_url"].rstrip("/") + "/v1beta/models/"
               + model["id"] + ":streamGenerateContent?alt=sse")
    else:
        url = (model["base_url"].rstrip("/") + "/v1beta/models/"
               + model["id"] + ":generateContent")
    _status, raw = http_raw("POST", url, body=body, headers=headers)
    return parse_provider_gemini(raw, stream)


# ---------------------------------------------------------------------------
# Routing / aggregator
# ---------------------------------------------------------------------------


def model_meets(model, needs):
    if needs.get("tools") and not model.get("tools"):
        return False
    if needs.get("vision") and not model.get("vision"):
        return False
    if needs.get("json") and not model.get("json"):
        return False
    min_ctx = needs.get("min_context")
    if min_ctx:
        ctx = model.get("context_length")
        if ctx is None or ctx < min_ctx:
            return False
    return True


def rank_candidates(models, needs, failed_providers):
    """Free models meeting the requirements, best first. A JSON-mode
    request never goes to an Anthropic-wire provider (that wire cannot
    carry JSON mode). Providers that refused 402 are excluded; providers
    that failed 3+ times are deprioritised below others within 50
    rating_milli."""
    out = []
    for m in models:
        if not m.get("free"):
            continue
        if needs.get("json") and m.get("wire") == "anthropic":
            continue
        if not model_meets(m, needs):
            continue
        if failed_providers.get(m["provider"], {}).get("exhausted"):
            continue
        out.append(m)
    fails = {p: v.get("count", 0) for p, v in failed_providers.items()}
    out.sort(key=lambda m: (
        -(m.get("rating_milli") or int(m.get("rating", 0) * 1000)),
        -(m.get("context_length") or 0),
        1 if fails.get(m["provider"], 0) >= 3 else 0,
        m.get("provider") or "", m.get("id") or "",
    ))
    return out


class Aggregator:
    def __init__(self, models, manifest):
        self.models = models
        self.manifest = manifest or []
        self.failed = {}  # provider -> {"count": n, "exhausted": bool}
        self.counter = 0
        self.request_index = 0
        self.lock = threading.Lock()

    def next_id(self, prefix):
        with self.lock:
            self.counter += 1
            return "%s-%d" % (prefix, self.counter)

    def requirements_for(self, req, raw_body):
        """Manifest row by arrival order, else body-derived."""
        idx = self.request_index
        self.request_index += 1
        if idx < len(self.manifest):
            row = self.manifest[idx] or {}
            needs = row.get("requires") or {}
            return {"tools": bool(needs.get("tools")),
                    "vision": bool(needs.get("vision")),
                    "json": bool(needs.get("json")),
                    "min_context": needs.get("min_context")}
        return derive_requirements(req, raw_body)

    def _note_failure(self, provider, status):
        entry = self.failed.setdefault(provider, {"count": 0, "exhausted": False})
        entry["count"] += 1
        if status == 402:
            entry["exhausted"] = True

    def serve(self, req, raw_body):
        """Call providers in ranked order until one answers. Returns
        (model, text, input_tokens, output_tokens, stop_reason)."""
        needs = self.requirements_for(req, raw_body)
        candidates = rank_candidates(self.models, needs, self.failed)
        # If the client named a real model id (e.g. a Gemini path model
        # or a mock test's "model" field) and it meets the requirements,
        # try it first; the real env sends model "makefun-router", which
        # matches nothing and is ignored.
        wanted = req.get("requested_model")
        if wanted:
            for m in candidates:
                if m["id"] == wanted:
                    candidates = [m] + [c for c in candidates if c is not m]
                    break
        if not candidates:
            # Relax: drop requirements one at a time (streaming is not a
            # requirement here; JSON is dropped last because the echo
            # check needs it — but a served answer beats no answer).
            for relax in ("min_context", "json", "vision", "tools"):
                relaxed = dict(needs)
                if relax == "min_context":
                    relaxed["min_context"] = None
                else:
                    relaxed[relax] = False
                candidates = rank_candidates(self.models, relaxed, self.failed)
                if candidates:
                    break
        last_error = None
        for model in candidates:
            try:
                text, u_in, u_out, stop = call_provider(model, req, req["stream"])
            except ProviderError as exc:
                self._note_failure(model["provider"], exc.status)
                last_error = exc
                continue
            except Exception as exc:
                self._note_failure(model["provider"], 500)
                last_error = exc
                continue
            return model, text, u_in, u_out, stop
        raise ProviderError(502, "no provider answered: %s" % (last_error,))


# ---------------------------------------------------------------------------
# Response emitters (client wire, JSON and SSE)
# ---------------------------------------------------------------------------


def _client_stop(stop, wire):
    """Map a provider stop reason to the client's wire."""
    if wire == "anthropic":
        return "end_turn" if stop in ("stop", "end_turn", "STOP") else stop
    return "stop" if stop in ("stop", "end_turn", "STOP") else stop


def build_openai_json(agg, model, text, u_in, u_out, stop):
    return {
        "id": agg.next_id("chatcmpl-agg"),
        "object": "chat.completion",
        "created": 0,
        "model": model["id"],
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": text},
            "finish_reason": _client_stop(stop, "openai"),
        }],
        "usage": {"prompt_tokens": u_in, "completion_tokens": u_out,
                  "total_tokens": u_in + u_out},
    }


def build_anthropic_json(agg, model, text, u_in, u_out, stop):
    return {
        "id": agg.next_id("msg-agg"),
        "type": "message",
        "role": "assistant",
        "model": model["id"],
        "content": [{"type": "text", "text": text}],
        "stop_reason": _client_stop(stop, "anthropic"),
        "stop_sequence": None,
        "usage": {"input_tokens": u_in, "output_tokens": u_out},
    }


def build_gemini_json(model, text, u_in, u_out, stop):
    return {
        "candidates": [{"content": {"role": "model",
                                    "parts": [{"text": text}]},
                        "finishReason": "STOP" if stop in ("stop", "end_turn", "STOP") else stop,
                        "index": 0}],
        "usageMetadata": {"promptTokenCount": u_in,
                          "candidatesTokenCount": u_out,
                          "totalTokenCount": u_in + u_out},
        "modelVersion": model["id"],
    }


def sse_events_openai(agg, model, text, u_in, u_out, stop):
    """OpenAI SSE: role chunk, content chunks, finish chunk, usage chunk,
    [DONE] — in the order the env's parser expects."""
    cid = agg.next_id("chatcmpl-agg")
    events = []
    events.append({"id": cid, "object": "chat.completion.chunk",
                   "created": 0, "model": model["id"],
                   "choices": [{"index": 0, "delta": {"role": "assistant"},
                                "finish_reason": None}]})
    for i in range(0, len(text), 100):
        events.append({"id": cid, "object": "chat.completion.chunk",
                       "created": 0, "model": model["id"],
                       "choices": [{"index": 0,
                                    "delta": {"content": text[i:i + 100]},
                                    "finish_reason": None}]})
    events.append({"id": cid, "object": "chat.completion.chunk",
                   "created": 0, "model": model["id"],
                   "choices": [{"index": 0, "delta": {},
                                "finish_reason": _client_stop(stop, "openai")}]})
    events.append({"id": cid, "object": "chat.completion.chunk",
                   "created": 0, "model": model["id"], "choices": [],
                   "usage": {"prompt_tokens": u_in, "completion_tokens": u_out,
                             "total_tokens": u_in + u_out}})
    return events


def sse_events_anthropic(agg, model, text, u_in, u_out, stop):
    """Anthropic SSE: message_start, content_block_start, deltas,
    content_block_stop, message_delta, message_stop."""
    mid = agg.next_id("msg-agg")
    events = []
    events.append(("message_start", {
        "type": "message_start",
        "message": {"id": mid, "type": "message", "role": "assistant",
                    "model": model["id"], "content": [],
                    "stop_reason": None, "stop_sequence": None,
                    "usage": {"input_tokens": u_in, "output_tokens": 0}}}))
    events.append(("content_block_start", {
        "type": "content_block_start", "index": 0,
        "content_block": {"type": "text", "text": ""}}))
    for i in range(0, len(text), 100):
        events.append(("content_block_delta", {
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "text_delta", "text": text[i:i + 100]}}))
    events.append(("content_block_stop", {
        "type": "content_block_stop", "index": 0}))
    events.append(("message_delta", {
        "type": "message_delta",
        "delta": {"stop_reason": _client_stop(stop, "anthropic"),
                  "stop_sequence": None},
        "usage": {"output_tokens": u_out}}))
    events.append(("message_stop", {"type": "message_stop"}))
    return events


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------


def free_model_ids(catalog):
    """Unique free model ids in catalog order (set comparison by scorer)."""
    seen = []
    for prov in catalog.get("providers") or []:
        for m in prov.get("models") or []:
            if m.get("free") and m.get("id") not in seen:
                seen.append(m["id"])
    return seen


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    agg = None
    catalog = {"providers": []}

    def log_message(self, *args):
        pass

    # -- helpers -----------------------------------------------------------
    def _send_json(self, status, obj, content_type="application/json"):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_sse(self, frames):
        body = b""
        for frame in frames:
            if isinstance(frame, tuple):
                event, payload = frame
                body += ("event: %s\ndata: %s\n\n" % (event, json.dumps(payload))).encode()
            else:
                body += ("data: %s\n\n" % json.dumps(frame)).encode()
        body += b"data: [DONE]\n\n" if not frames or not isinstance(frames[0], tuple) else b""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b""
        try:
            return json.loads(raw.decode("utf-8")) if raw else {}
        except Exception:
            return {}

    # -- routes ------------------------------------------------------------
    def do_GET(self):
        if self.path == "/v1/models":
            self._handle_models()
        elif self.path in ("/", "/health"):
            self._send_json(200, {"status": "ok", "version": VERSION})
        else:
            self._send_json(404, {"error": {"message": "not found"}})

    def _handle_models(self):
        ids = free_model_ids(self.catalog)
        if self.headers.get("x-api-key"):
            data = [{"type": "model", "id": mid, "display_name": mid,
                     "created_at": "2026-01-01T00:00:00Z"} for mid in ids]
            self._send_json(200, {"data": data, "has_more": False})
        else:
            data = [{"id": mid, "object": "model",
                     "created": MODEL_LIST_CREATED,
                     "owned_by": "free-model-aggregator"} for mid in ids]
            self._send_json(200, {"object": "list", "data": data})

    def do_POST(self):
        body = self._read_body()
        if self.path == "/v1/chat/completions":
            self._handle_chat("openai", body)
        elif self.path == "/v1/messages":
            self._handle_chat("anthropic", body)
        elif self.path.startswith("/v1beta/models/") and self.path.endswith(":generateContent"):
            self._handle_gemini(body, False)
        elif self.path.startswith("/v1beta/models/") and self.path.endswith(":streamGenerateContent"):
            self._handle_gemini(body, True)
        else:
            self._send_json(404, {"error": {"message": "not found"}})

    def _handle_chat(self, wire, body):
        req = parse_client_request(wire, body)
        try:
            model, text, u_in, u_out, stop = self.agg.serve(req, body)
        except ProviderError as exc:
            self._send_json(502, {"error": {"message": str(exc)}})
            return
        if wire == "openai":
            if req["stream"]:
                frames = sse_events_openai(self.agg, model, text, u_in, u_out, stop)
                self._send_sse(frames)
            else:
                self._send_json(200, build_openai_json(self.agg, model, text, u_in, u_out, stop))
        else:
            if req["stream"]:
                frames = sse_events_anthropic(self.agg, model, text, u_in, u_out, stop)
                self._send_sse(frames)
            else:
                self._send_json(200, build_anthropic_json(self.agg, model, text, u_in, u_out, stop))

    def _handle_gemini(self, body, stream):
        # Gemini client wire: contents -> canonical, served like any other.
        req = {"wire": "gemini", "stream": stream, "system": "",
               "messages": [], "tools": [], "tool_names": [],
               "max_tokens": None, "temperature": None, "top_p": None,
               "stops": [], "json_mode": False, "raw": body,
               "requested_model": self.path.split("/models/")[-1].split(":")[0]
               if "/models/" in self.path else None}
        sys_inst = body.get("systemInstruction") or {}
        for part in sys_inst.get("parts") or []:
            if isinstance(part.get("text"), str):
                req["system"] = (req["system"] + "\n" + part["text"]).strip() if req["system"] else part["text"]
        for content in body.get("contents") or []:
            parts = []
            for part in content.get("parts") or []:
                if isinstance(part.get("text"), str):
                    parts.append({"type": _PART_TEXT, "text": part["text"]})
                elif "inlineData" in part:
                    d = part["inlineData"]
                    parts.append({"type": _PART_IMAGE,
                                  "url": "data:%s;base64,%s" % (d.get("mimeType", "image/png"), d.get("data", ""))})
            req["messages"].append({"role": content.get("role", "user"), "parts": parts})
        for tool in body.get("tools") or []:
            for decl in tool.get("functionDeclarations") or []:
                req["tool_names"].append(decl.get("name"))
                req["tools"].append({"function": decl})
        cfg = body.get("generationConfig") or {}
        req["max_tokens"] = cfg.get("maxOutputTokens")
        req["temperature"] = cfg.get("temperature")
        req["top_p"] = cfg.get("topP")
        req["stops"] = cfg.get("stopSequences") or []
        req["json_mode"] = cfg.get("responseMimeType") == "application/json"
        try:
            model, text, u_in, u_out, stop = self.agg.serve(req, body)
        except ProviderError as exc:
            self._send_json(502, {"error": {"message": str(exc)}})
            return
        if stream:
            # Gemini stream: single SSE frame with the full answer.
            frame = build_gemini_json(model, text, u_in, u_out, stop)
            self._send_sse([frame])
        else:
            self._send_json(200, build_gemini_json(model, text, u_in, u_out, stop))


# ---------------------------------------------------------------------------
# Registration + work polling
# ---------------------------------------------------------------------------


def register(env_url, port, catalog):
    """POST /register with protocol_version 1. Returns True on 200."""
    if not env_url:
        return False
    try:
        status, doc, _raw = http_json(
            "POST", env_url.rstrip("/") + "/register",
            body={"protocol_version": 1, "port": port, "catalog": catalog},
            timeout=10)
        return status == 200
    except Exception as exc:
        print("register failed: %s" % exc, file=sys.stderr, flush=True)
        return False


def poll_work(env_url, server):
    """Poll GET /work every 100 ms; shut the server down and exit 0
    when the env says the workload is done."""
    if not env_url:
        return
    url = env_url.rstrip("/") + "/work"
    while True:
        time.sleep(0.1)
        try:
            _status, doc, _raw = http_json("GET", url, timeout=5)
        except Exception:
            continue
        if isinstance(doc, dict) and doc.get("done"):
            break
    server.shutdown()
    os._exit(0)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="free-model aggregator")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--host", default=None)
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    env_url = os.environ.get("MAKEFUN_ENV_URL", "").rstrip("/")
    providers_doc, ratings_doc, workload_doc = load_brief()
    manifest = []
    if workload_doc:
        manifest = workload_doc.get("manifest") or workload_doc.get("requests") or []
    if providers_doc is not None:
        catalog, models = build_catalog(env_url, providers_doc, ratings_doc)
    else:
        catalog, models = discover_fallback(env_url)

    if args.list:
        sys.stdout.buffer.write(catalog_bytes(catalog))
        sys.stdout.flush()
        return 0

    host = args.host or os.environ.get("MAKEFUN_HOST", "0.0.0.0")
    port = args.port or int(os.environ.get("MAKEFUN_PORT", "0"))
    Handler.agg = Aggregator(models, manifest)
    Handler.catalog = catalog
    server = ThreadingHTTPServer((host, port), Handler)
    port = server.server_address[1]
    print("aggregator listening on %s:%d, %d models, %d providers"
          % (host, port, len(models),
             len(catalog.get("providers") or [])), flush=True)
    if env_url:
        if register(env_url, port, catalog):
            print("registered with %s" % env_url, flush=True)
        threading.Thread(target=poll_work, args=(env_url, server),
                         daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
