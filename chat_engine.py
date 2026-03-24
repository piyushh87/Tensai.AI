import json
import os
import re
import urllib.request
import urllib.error
from urllib.parse import urlparse

# Attempt to load OSINT Web Crawler dependency
try:
    from duckduckgo_search import DDGS
    DDGS_AVAILABLE = True
except ImportError:
    DDGS_AVAILABLE = False

# Attempt to load yfinance for live stock market data
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False

# Prefer a server-side environment variable, but keep the existing fallback so the app
# does not silently lose functionality in local development.
HARDCODED_API_KEY = os.getenv("OPENROUTER_API_KEY", "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")
OPENROUTER_URL = "https://" + "openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODEL = "openai/gpt-4o-mini"
MAX_CONTEXT_CHARS = 60000
DEFAULT_RESPONSE_MAX_TOKENS = 900
DETAILED_RESPONSE_MAX_TOKENS = 1400
FOLLOW_UP_RESPONSE_MAX_TOKENS = 1000
MIN_RESPONSE_MAX_TOKENS = 256
RETRY_OUTPUT_BUFFER = 64


def _extract_provider_message(error_body):
    try:
        payload = json.loads(error_body)
        if isinstance(payload, dict):
            error = payload.get("error")
            if isinstance(error, dict) and error.get("message"):
                return str(error["message"]).strip()
        return str(payload).strip()
    except Exception:
        return str(error_body or "").strip()


def _extract_affordable_token_limit(error_body):
    message = _extract_provider_message(error_body)
    match = re.search(r"can only afford\s+(\d+)", message, re.IGNORECASE)
    return int(match.group(1)) if match else None


def _choose_response_token_budget(query, context_text="", history=None, is_follow_up=False):
    prompt = str(query or "").lower()
    wants_detail = any(phrase in prompt for phrase in (
        "full detail",
        "full details",
        "detailed",
        "comprehensive",
        "complete analysis",
        "deep dive",
        "thorough",
    ))
    wants_summary = any(phrase in prompt for phrase in (
        "summary",
        "summarize",
        "key insights",
        "main points",
        "overview",
        "highlights",
    ))

    if wants_detail:
        budget = DETAILED_RESPONSE_MAX_TOKENS
    elif wants_summary:
        budget = 1000
    else:
        budget = DEFAULT_RESPONSE_MAX_TOKENS

    if is_follow_up:
        budget = min(budget, FOLLOW_UP_RESPONSE_MAX_TOKENS)
    if len(str(context_text or "")) > 45000:
        budget = min(budget, 1000 if wants_detail else 850)
    if history and len(history) > 6:
        budget = min(budget, 850)

    return max(MIN_RESPONSE_MAX_TOKENS, budget)


def _is_forward_projection_query(query):
    prompt = str(query or "").lower()
    future_terms = (
        "forecast",
        "project",
        "projection",
        "predict",
        "prediction",
        "estimate",
        "outlook",
        "guidance",
        "future",
        "next year",
        "next 5 years",
        "after 5 years",
        "five years",
        "in 5 years",
        "after five years",
        "in five years",
    )
    metric_terms = (
        "revenue",
        "revenw",
        "sales",
        "earnings",
        "eps",
        "margin",
        "profit",
        "cash flow",
        "free cash flow",
        "fcf",
        "growth",
    )

    has_future_term = any(term in prompt for term in future_terms)
    has_metric_term = any(term in prompt for term in metric_terms)
    has_year_horizon = bool(re.search(r"\b(?:next|after|in)\s+(?:\d+|one|two|three|four|five|six|seven|eight|nine|ten)\s+years?\b", prompt))

    return has_metric_term and (has_future_term or has_year_horizon)


def _build_effective_query(query):
    raw_query = str(query or "").strip()
    if not _is_forward_projection_query(raw_query):
        return raw_query

    forecast_guidance = """
FORECAST MODE ACTIVE:
- The user is explicitly asking for a future financial estimate.
- Do not stop at "Not stated in the document" if the document contains enough evidence to build a defensible forecast.
- Build an evidence-based scenario forecast using only the document's disclosed figures, trends, risks, strategy, and management commentary.
- Present the answer as an estimate, not as confirmed fact.
- Give a **base case** first. Include **bull case** and **bear case** ranges when possible.
- State the exact assumptions behind the forecast and cite the document evidence that supports them.
- If the document is too thin for a numeric forecast, say that directly, then provide the closest defensible scenario framework instead of refusing with only a missing-data statement.
- Keep the final output structured and practical.
"""
    return f"{raw_query}\n\n{forecast_guidance.strip()}"


def _format_provider_error(status_code, error_body, requested_tokens):
    message = _extract_provider_message(error_body)
    affordable_tokens = _extract_affordable_token_limit(error_body)

    if status_code == 402:
        if affordable_tokens:
            return (
                "> **AI budget limit reached**: OpenRouter rejected this request because the account "
                f"cannot fund the requested response size. The backend asked for up to **{requested_tokens}** "
                f"output tokens, but the account can currently afford about **{affordable_tokens}**. "
                "Try a shorter question, a smaller document, or add OpenRouter credits."
            )
        return (
            "> **AI budget limit reached**: OpenRouter rejected this request because the current "
            "account balance is too low. Try a shorter question, a smaller document, or add OpenRouter credits."
        )

    if message:
        return f"> **Backend Connection Error**: HTTP {status_code}. {message}"
    return f"> **Backend Connection Error**: HTTP {status_code}."


def _request_openrouter_completion(headers, messages, query="", context_text="", history=None, tools=None, tool_choice=None, temperature=0.4, max_tokens=None):
    requested_tokens = max_tokens or _choose_response_token_budget(query, context_text, history)
    payload = {
        "model": OPENROUTER_MODEL,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": requested_tokens,
    }

    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = tool_choice or "auto"

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(OPENROUTER_URL, data=data, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        affordable_tokens = _extract_affordable_token_limit(error_body)
        can_retry = (
            exc.code == 402
            and affordable_tokens
            and affordable_tokens >= MIN_RESPONSE_MAX_TOKENS
            and affordable_tokens < requested_tokens
        )

        if can_retry:
            retry_tokens = max(MIN_RESPONSE_MAX_TOKENS, affordable_tokens - RETRY_OUTPUT_BUFFER)
            if retry_tokens < requested_tokens:
                payload["max_tokens"] = retry_tokens
                retry_data = json.dumps(payload).encode("utf-8")
                retry_req = urllib.request.Request(OPENROUTER_URL, data=retry_data, headers=headers, method="POST")
                try:
                    with urllib.request.urlopen(retry_req) as response:
                        return json.loads(response.read().decode("utf-8"))
                except urllib.error.HTTPError as retry_exc:
                    retry_body = retry_exc.read().decode("utf-8", errors="replace")
                    raise RuntimeError(_format_provider_error(retry_exc.code, retry_body, retry_tokens)) from retry_exc

        raise RuntimeError(_format_provider_error(exc.code, error_body, requested_tokens)) from exc

def _tokenize_text(value):
    return [token for token in re.findall(r"[a-z0-9]+", str(value or "").lower()) if len(token) > 1]


def _clean_context_value(value):
    text = str(value or "").strip()
    if text.lower() in {"", "n/a", "none", "unknown", "unassigned", "not specified", "unavailable"}:
        return ""
    return text


def _split_context_items(value, limit=8):
    items = []
    seen = set()
    for raw in re.split(r"[\n,;|]+", str(value or "")):
        clean = _clean_context_value(raw)
        key = clean.lower()
        if clean and key not in seen:
            seen.add(key)
            items.append(clean)
        if len(items) >= limit:
            break
    return items


def _extract_osint_profile(context_text, fallback_name=""):
    profile = {
        "name": _clean_context_value(fallback_name),
        "email": "",
        "location": "",
        "role": "",
        "companies": [],
        "skills": [],
        "resume_excerpt": "",
    }

    for line in str(context_text or "").splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip().lower()
        value = _clean_context_value(value)
        if not value:
            continue

        if key == "name":
            profile["name"] = value
        elif key in {"email", "emails"}:
            profile["email"] = value.split(",")[0].strip()
        elif key in {"location", "city", "region"}:
            profile["location"] = value
        elif key in {"role", "roles", "job title", "job titles", "target role"}:
            profile["role"] = value
        elif key in {"company", "companies", "employers"}:
            profile["companies"] = _split_context_items(value, limit=4)
        elif key == "skills":
            profile["skills"] = _split_context_items(value, limit=8)
        elif key in {"resume excerpt", "summary"}:
            profile["resume_excerpt"] = value[:240]

    if profile["email"]:
        local_part, _, domain = profile["email"].partition("@")
        profile["email_user"] = re.sub(r"[^a-z0-9]+", " ", local_part.lower()).strip()
        profile["email_domain"] = domain.lower().strip()
    else:
        profile["email_user"] = ""
        profile["email_domain"] = ""

    name_tokens = _tokenize_text(profile["name"])
    profile["name_tokens"] = name_tokens
    profile["first_name"] = name_tokens[0] if name_tokens else ""
    profile["surname"] = name_tokens[-1] if len(name_tokens) > 1 else ""
    return profile


def _build_osint_queries(target_name, platform, profile):
    name = _clean_context_value(target_name) or profile.get("name") or "candidate"
    role = profile.get("role", "")
    location = profile.get("location", "")
    company = (profile.get("companies") or [""])[0]
    email_user = profile.get("email_user", "")
    queries = []

    if platform == "linkedin":
        queries.extend([
            f'"{name}" site:linkedin.com/in',
            f'"{name}" site:linkedin.com/pub',
        ])
        if role:
            queries.append(f'"{name}" "{role}" site:linkedin.com/in')
        if company:
            queries.append(f'"{name}" "{company}" site:linkedin.com/in')
        if location:
            queries.append(f'"{name}" "{location}" site:linkedin.com/in')
        if email_user:
            queries.append(f'"{name}" "{email_user}" site:linkedin.com/in')
    elif platform == "github":
        queries.append(f'"{name}" site:github.com')
        if role:
            queries.append(f'"{name}" github "{role}"')
        if company:
            queries.append(f'"{name}" github "{company}"')
        if email_user:
            queries.append(f'"{name}" github "{email_user}"')
    else:
        queries.append(f'"{name}" ("portfolio" OR "resume" OR "personal website" OR "contact")')
        if role:
            queries.append(f'"{name}" "{role}" ("portfolio" OR "resume")')
        if company:
            queries.append(f'"{name}" "{company}" ("portfolio" OR "about")')
        if location:
            queries.append(f'"{name}" "{location}" ("portfolio" OR "resume")')

    unique_queries = []
    seen = set()
    for query in queries:
        if query not in seen:
            seen.add(query)
            unique_queries.append(query)
    return unique_queries[:5]


def _score_osint_result(result, platform, profile):
    href = (result.get("href") or result.get("url") or "").strip()
    title = (result.get("title") or "").strip()
    body = (result.get("body") or "").strip()
    combined = f"{title} {body} {href}".lower()
    path = urlparse(href).path.lower() if href else ""

    score = 0
    signals = []
    reasons = []

    if platform == "linkedin" and "linkedin.com/" in href.lower():
        score += 24
        signals.append("linkedin domain")
    elif platform == "github" and "github.com/" in href.lower():
        score += 24
        signals.append("github domain")
    elif platform == "general" and href:
        score += 8
        signals.append("reachable web result")

    full_name = profile.get("name", "").lower().strip()
    first_name = profile.get("first_name", "")
    surname = profile.get("surname", "")
    role_tokens = _tokenize_text(profile.get("role", ""))[:4]
    location_tokens = _tokenize_text(profile.get("location", ""))[:3]
    company_tokens = []
    for company in (profile.get("companies") or [])[:2]:
        company_tokens.extend(_tokenize_text(company)[:2])
    email_user_tokens = _tokenize_text(profile.get("email_user", ""))[:3]
    skill_tokens = []
    for skill in (profile.get("skills") or [])[:4]:
        skill_tokens.extend(_tokenize_text(skill)[:2])

    if full_name and full_name in combined:
        score += 52
        signals.append("exact full name")
    if first_name and first_name in combined:
        score += 12
        signals.append("first name")
    if surname:
        if surname in combined:
            score += 24
            signals.append("surname")
        elif first_name:
            score -= 30
            reasons.append("surname missing")

    if platform == "github" and profile.get("email_user"):
        email_hint = profile["email_user"].replace(" ", "")
        if email_hint and email_hint in combined.replace(" ", ""):
            score += 16
            signals.append("email alias match")

    role_hits = [token for token in role_tokens if token in combined]
    if role_hits:
        score += min(18, len(role_hits) * 6)
        signals.append(f"role hint: {', '.join(role_hits[:2])}")

    company_hits = [token for token in company_tokens if token in combined]
    if company_hits:
        score += min(16, len(company_hits) * 8)
        signals.append(f"company hint: {', '.join(company_hits[:2])}")

    location_hits = [token for token in location_tokens if token in combined]
    if location_hits:
        score += min(10, len(location_hits) * 5)
        signals.append(f"location hint: {', '.join(location_hits[:2])}")

    skill_hits = [token for token in skill_tokens if token in combined]
    if skill_hits:
        score += min(10, len(skill_hits) * 5)
        signals.append(f"skill hint: {', '.join(skill_hits[:2])}")

    if any(word in combined for word in ["minister", "politician", "government", "parliament"]) and surname and surname not in combined:
        score -= 25
        reasons.append("public figure mismatch")

    if platform == "linkedin" and "/in/" in path:
        score += 8
    if platform == "github" and path.count("/") <= 2:
        score += 8

    confidence = "high" if score >= 80 else "medium" if score >= 55 else "low"
    return {
        "title": title or href,
        "href": href,
        "snippet": body[:220],
        "score": score,
        "confidence": confidence,
        "matched_signals": signals[:5],
        "reasons": reasons[:3],
        "query_used": result.get("query_used", ""),
    }


def _run_osint_trace(target_name, platform, supporting_context):
    profile = _extract_osint_profile(supporting_context, fallback_name=target_name)
    queries = _build_osint_queries(target_name, platform, profile)

    if not DDGS_AVAILABLE:
        return {
            "platform": platform,
            "target_name": profile.get("name") or target_name,
            "status": "tool_offline",
            "message": "Crawler offline. System Administrator needs to run 'pip install duckduckgo-search'.",
            "queries": queries,
            "top_results": [],
            "rejected_examples": [],
        }

    collected = []
    with DDGS() as ddgs:
        for query in queries:
            try:
                for item in ddgs.text(query, max_results=6):
                    item["query_used"] = query
                    collected.append(item)
            except Exception:
                continue

    ranked = {}
    for item in collected:
        scored = _score_osint_result(item, platform, profile)
        if not scored["href"]:
            continue
        existing = ranked.get(scored["href"])
        if not existing or scored["score"] > existing["score"]:
            ranked[scored["href"]] = scored

    ordered = sorted(ranked.values(), key=lambda row: row["score"], reverse=True)
    top_results = [row for row in ordered if row["score"] >= 55][:3]
    rejected_examples = [
        {"title": row["title"], "href": row["href"], "reason": ", ".join(row["reasons"]) or "weak identity match"}
        for row in ordered if row["score"] < 40
    ][:3]

    if top_results and top_results[0]["score"] >= 80:
        status = "strong_match"
        message = "High-confidence candidate-aligned records identified."
    elif top_results:
        status = "possible_match"
        message = "Possible records found, but corroborating identity signals are limited."
    else:
        status = "no_confident_match"
        message = "No high-confidence records found after disambiguation and false-positive filtering."

    identity_hints = {}
    for key in ("email", "location", "role"):
        if profile.get(key):
            identity_hints[key] = profile[key]
    if profile.get("companies"):
        identity_hints["companies"] = profile["companies"][:2]
    if profile.get("skills"):
        identity_hints["skills"] = profile["skills"][:4]

    return {
        "platform": platform,
        "target_name": profile.get("name") or target_name,
        "status": status,
        "message": message,
        "queries": queries,
        "identity_hints": identity_hints,
        "top_results": top_results,
        "rejected_examples": rejected_examples,
        "result_count": len(ordered),
    }


def build_osint_bundle(target_name, context_text):
    profile = _extract_osint_profile(context_text, fallback_name=target_name)
    platforms = [_run_osint_trace(profile.get("name") or target_name, platform, context_text) for platform in ("linkedin", "github", "general")]

    strong_matches = sum(1 for item in platforms if item.get("status") == "strong_match")
    possible_matches = sum(1 for item in platforms if item.get("status") == "possible_match")
    top_results = sum(len(item.get("top_results") or []) for item in platforms)
    rejected = sum(len(item.get("rejected_examples") or []) for item in platforms)

    if strong_matches >= 2:
        overall_status = "verified_presence"
        confidence = "high"
        headline = "Strong cross-platform identity signals found."
        detail = "Multiple platforms returned candidate-aligned records with corroborating identity clues."
    elif strong_matches == 1 or possible_matches >= 2:
        overall_status = "partial_presence"
        confidence = "medium"
        headline = "Partial digital footprint identified."
        detail = "Some likely records were found, but at least one platform still needs manual confirmation."
    elif possible_matches == 1:
        overall_status = "weak_presence"
        confidence = "medium-low"
        headline = "A weak or ambiguous footprint was detected."
        detail = "Only limited records survived disambiguation, so confidence remains constrained."
    else:
        overall_status = "no_confident_presence"
        confidence = "low"
        headline = "No high-confidence public footprint found."
        detail = "Searches were executed, but the evidence did not meet the confidence threshold for a reliable match."

    next_steps = []
    if not profile.get("email"):
        next_steps.append("Add a verified email or username to improve identity matching.")
    if not profile.get("location"):
        next_steps.append("Include location context to filter common-name collisions.")
    if not profile.get("role"):
        next_steps.append("Add a current role or target title to sharpen platform queries.")
    if overall_status in {"partial_presence", "weak_presence"}:
        next_steps.append("Manually inspect the highest-ranked links before using them in hiring decisions.")
    if overall_status == "no_confident_presence":
        next_steps.append("Try alternate candidate aliases or known usernames from the resume.")
    if not next_steps:
        next_steps.append("Use the top-ranked records to validate employment chronology and public project evidence.")

    return {
        "target_name": profile.get("name") or target_name,
        "profile": {
            "name": profile.get("name", ""),
            "email": profile.get("email", ""),
            "location": profile.get("location", ""),
            "role": profile.get("role", ""),
            "companies": profile.get("companies", []),
            "skills": profile.get("skills", [])[:8],
        },
        "summary": {
            "status": overall_status,
            "confidence": confidence,
            "headline": headline,
            "detail": detail,
            "matched_platforms": strong_matches + possible_matches,
            "reviewed_results": sum(item.get("result_count", 0) for item in platforms),
            "top_results": top_results,
            "rejected_results": rejected,
        },
        "platforms": platforms,
        "next_steps": next_steps[:4],
    }


def _build_openrouter_headers(active_api_key):
    return {
        "Authorization": f"Bearer {active_api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "http://localhost:5000",
        "X-Title": "TalentAI OS",
    }


def _iter_text_parts(content):
    if isinstance(content, str):
        if content:
            yield content
        return
    if isinstance(content, list):
        for part in content:
            if isinstance(part, str):
                if part:
                    yield part
            elif isinstance(part, dict):
                text = str(part.get("text", "") or "").strip()
                if text:
                    yield text


def _extract_message_text(message):
    if not isinstance(message, dict):
        return ""
    return "".join(_iter_text_parts(message.get("content", ""))).strip()


def _normalize_history_messages(history):
    normalized = []
    for msg in (history or [])[-6:]:
        if not isinstance(msg, dict) or "tool_calls" in msg:
            continue
        role = str(msg.get("role", "user")).strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = _extract_message_text(msg)
        if not content:
            continue
        normalized.append({"role": role, "content": content[:15000]})
    return normalized


def _build_chat_tools():
    return [
        {
            "type": "function",
            "function": {
                "name": "perform_deep_osint_trace",
                "description": "Performs a deep Open Source Intelligence (OSINT) scan on a candidate using advanced search operators (Dorks) targeting specific platforms.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "target_name": {"type": "string", "description": "The exact name or handle of the target candidate."},
                        "platform": {
                            "type": "string",
                            "enum": ["linkedin", "github", "general"],
                            "description": "The specific intelligence vector/platform to scan."
                        },
                        "supporting_context": {
                            "type": "string",
                            "description": "Optional identity clues from the resume or user prompt, such as role, email, location, company, and skills."
                        }
                    },
                    "required": ["target_name", "platform"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "get_stock_price",
                "description": "Fetches the current live stock price, cryptocurrency, or commodity market data for a given ticker symbol (e.g., AAPL for Apple, GC=F for Gold, BTC-USD for Bitcoin).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ticker": {"type": "string", "description": "The financial ticker symbol (e.g., TSLA, GC=F, BTC-USD)."}
                    },
                    "required": ["ticker"]
                }
            }
        }
    ]


def _infer_document_analysis_mode(context_text):
    prompt = str(context_text or "").lower()
    financial_terms = (
        "annual report", "10-k", "10-q", "earnings", "revenue", "net income", "operating income",
        "cash flow", "balance sheet", "gross margin", "eps", "segment", "shareholders", "fiscal year",
    )
    resume_terms = (
        "--- resume ---", "job description", "candidate", "experience", "education", "skills",
        "certifications", "work history", "professional summary", "resume", "projects",
    )
    financial_hits = sum(1 for term in financial_terms if term in prompt)
    resume_hits = sum(1 for term in resume_terms if term in prompt)
    if financial_hits >= max(2, resume_hits + 1):
        return "financial"
    if resume_hits >= max(2, financial_hits + 1):
        return "resume"
    return "unknown"


def _mode_restriction_message(analysis_mode):
    if analysis_mode == "resume":
        return "> **Mode Restriction**: Resume Analysis only answers questions grounded in resumes and job descriptions. Switch to AI Financial Analysis for report work."
    return "> **Mode Restriction**: AI Financial Analysis only answers questions grounded in uploaded financial reports. Switch to Resume Analysis for candidate work."


def _build_document_chat_system_prompt(analysis_mode="financial", interview_mode=False, is_forecast_request=False):
    if analysis_mode == "resume":
        system_prompt = """You are 'NEURO-LINK V2.4' operating in TALENT-OS Resume Analysis mode.
The user has uploaded resume and hiring material, and the extracted text is provided in DOCUMENT CONTEXT below. Treat DOCUMENT CONTEXT as the only source of truth.

MODE RULES:
- Only analyze resumes, job descriptions, candidate dossiers, and hiring evidence grounded in DOCUMENT CONTEXT.
- Do not answer financial-report, annual report, earnings, investor, stock, or company-filing analysis in this mode.
- If the user asks for financial report analysis or the document is not resume-oriented, reply with this exact sentence:
> **Mode Restriction**: Resume Analysis only answers questions grounded in resumes and job descriptions. Switch to AI Financial Analysis for report work.

GROUNDING AND ACCURACY RULES:
1. Use only facts supported by DOCUMENT CONTEXT.
2. Do not invent figures, dates, names, skills, events, companies, or conclusions.
3. If information is missing for a factual question, say exactly: "Not stated in the document."
4. If you make a limited inference, label it clearly as "Inference" and explain the supporting evidence.
5. Prefer exact roles, skills, dates, achievements, and concrete phrases from the document whenever available.
6. Separate confirmed findings from concerns, assumptions, and open questions.
7. Keep claims proportional to the evidence. Do not overstate certainty.

FORMATTING RULES:
- Always start major responses with: ### SYS.UPLINK: SECURE
- Use strict Markdown only.
- Use bullet points (-) for findings, risks, and recommendations.
- Prefer this order when relevant: Document Classification, Executive Summary, Key Evidence, Strengths, Gaps, Recommendations, Open Questions.
"""
    else:
        system_prompt = """You are 'NEURO-LINK V2.4' operating in TALENT-OS AI Financial Analysis mode.
The user has uploaded a financial report, and the extracted text is provided in DOCUMENT CONTEXT below. Treat DOCUMENT CONTEXT as the only source of truth.

MODE RULES:
- Only analyze annual reports, earnings releases, 10-Ks, 10-Qs, investor updates, budgets, and other financial/business reporting documents grounded in DOCUMENT CONTEXT.
- Do not answer resume analysis, candidate-fit, hiring, interview, skill-gap, or talent questions in this mode.
- Do not use web, OSINT, market-price, or external-data tools in this mode.
- If the user asks for resume or candidate analysis or the document is not clearly financial, reply with this exact sentence:
> **Mode Restriction**: AI Financial Analysis only answers questions grounded in uploaded financial reports. Switch to Resume Analysis for candidate work.

GROUNDING AND ACCURACY RULES:
1. Use only facts supported by DOCUMENT CONTEXT.
2. Do not invent figures, dates, names, segments, metrics, or conclusions.
3. If information is missing for a factual question, say exactly: "Not stated in the document."
4. If you make a limited inference, label it clearly as "Inference" and explain the supporting evidence.
5. Prefer exact figures, periods, metrics, and phrases from the report whenever available.
6. Separate confirmed findings from concerns, assumptions, and open questions.
7. Keep claims proportional to the evidence. Do not overstate certainty.

FORMATTING RULES:
- Always start major responses with: ### SYS.UPLINK: SECURE
- Use strict Markdown only.
- Use bullet points (-) for findings, risks, and recommendations.
- Prefer this order when relevant: Document Classification, Executive Summary, Key Evidence, Drivers, Risks, Recommendations, Open Questions.
"""

    if analysis_mode == "resume" and interview_mode:
        system_prompt += """

TECHNICAL INTERVIEWER MODE:
- Behave as a rigorous technical interviewer reviewing the uploaded candidate material.
- Interrogate the candidate's document claims instead of summarizing them passively.
- Prioritize data management, analytics workflows, reporting and visualization depth, framework fluency, debugging skill, delivery ownership, and production tradeoffs whenever the document supports those lines of questioning.
- If the user asks for interview questions, provide a sharp, challenging set of questions grounded in the document, plus what strong answers should cover.
- Explicitly challenge vague ownership claims, missing metrics, shallow framework references, and weak evidence.
- Keep the tone demanding, specific, and evidence-backed, while still using Markdown and the system header.
"""

    if analysis_mode == "financial" and is_forecast_request:
        system_prompt += """

FORECAST PROTOCOL:
- The user is explicitly asking for a forward-looking financial estimate.
- You may provide a scenario-based forecast when DOCUMENT CONTEXT contains enough evidence to support one.
- Treat the document as the only evidence base unless a tool returns additional data.
- Clearly label all forecast outputs as estimates, not confirmed facts.
- Provide a **Base Case** first, and include **Bull Case** and **Bear Case** ranges when the evidence supports ranges.
- Show the assumptions driving the estimate, using concrete figures, trends, risks, margins, growth rates, or management commentary from the document.
- If the document is too thin for a numeric projection, say **Not enough evidence in the document for a defensible numeric forecast**, then provide the closest useful scenario framework instead of stopping with a refusal.
- Include a short confidence label such as **Low**, **Medium**, or **High** and explain why.
"""
    return system_prompt


def _build_document_chat_messages(query, context_text, history=None, interview_mode=False, analysis_mode="financial"):
    raw_query = str(query or "").strip()
    is_forecast_request = analysis_mode == "financial" and _is_forward_projection_query(raw_query) and not interview_mode
    effective_query = _build_effective_query(raw_query) if is_forecast_request else raw_query
    optimized_context_text = str(context_text or "")[:MAX_CONTEXT_CHARS]
    system_prompt = _build_document_chat_system_prompt(
        analysis_mode=analysis_mode,
        interview_mode=interview_mode,
        is_forecast_request=is_forecast_request,
    )
    messages = [{
        "role": "system",
        "content": system_prompt + f"\n\n--- DOCUMENT CONTEXT ---\n{optimized_context_text}\n--- END OF DOCUMENT CONTEXT ---",
    }]
    messages.extend(_normalize_history_messages(history))
    messages.append({"role": "user", "content": effective_query})
    return messages, effective_query, optimized_context_text


def _resolve_tool_call_messages(tool_calls, optimized_context_text):
    resolved_messages = []
    for tool_call in tool_calls or []:
        func_name = str(tool_call.get("function", {}).get("name", "")).strip()
        tool_result = "Unknown subroutine requested."

        if func_name == "perform_deep_osint_trace":
            try:
                args = json.loads(tool_call["function"]["arguments"])
                target_name = args.get("target_name", "")
                platform = args.get("platform", "general")
                supporting_context = args.get("supporting_context") or optimized_context_text
                tool_result = json.dumps(_run_osint_trace(target_name, platform, supporting_context))
            except Exception as exc:
                tool_result = f"OSINT Exception: {str(exc)}"

        elif func_name == "get_stock_price":
            try:
                args = json.loads(tool_call["function"]["arguments"])
                ticker = args.get("ticker", "")
                if YFINANCE_AVAILABLE:
                    stock = yf.Ticker(ticker)
                    current_price = stock.fast_info.last_price
                    prev_close = stock.fast_info.previous_close
                    diff = current_price - prev_close
                    pct_change = (diff / prev_close) * 100 if prev_close else 0
                    tool_result = json.dumps({
                        "ticker": ticker,
                        "current_price": round(current_price, 2),
                        "change": round(diff, 2),
                        "percent_change": round(pct_change, 2),
                        "currency": "USD",
                    })
                else:
                    tool_result = "Finance module offline. System Administrator needs to run 'pip install yfinance'."
            except Exception as exc:
                tool_result = f"Finance Exception: {str(exc)}"

        resolved_messages.append({
            "role": "tool",
            "tool_call_id": tool_call.get("id", ""),
            "name": func_name or "unknown_tool",
            "content": tool_result,
        })
    return resolved_messages


def _request_openrouter_stream(headers, messages, query="", context_text="", history=None, tools=None, tool_choice=None, temperature=0.4, max_tokens=None):
    requested_tokens = max_tokens or _choose_response_token_budget(query, context_text, history)
    payload = {
        "model": OPENROUTER_MODEL,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": requested_tokens,
        "stream": True,
    }
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = tool_choice or "auto"

    def _stream_payload(active_payload, active_requested_tokens):
        data = json.dumps(active_payload).encode("utf-8")
        req = urllib.request.Request(OPENROUTER_URL, data=data, headers=headers, method="POST")

        try:
            with urllib.request.urlopen(req) as response:
                for raw_line in response:
                    line = raw_line.decode("utf-8", errors="replace").strip()
                    if not line or not line.startswith("data:"):
                        continue
                    chunk = line[5:].strip()
                    if chunk == "[DONE]":
                        break
                    try:
                        payload_json = json.loads(chunk)
                    except Exception:
                        continue
                    for choice in payload_json.get("choices") or []:
                        delta = choice.get("delta") or {}
                        for text_part in _iter_text_parts(delta.get("content")):
                            if text_part:
                                yield text_part
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            affordable_tokens = _extract_affordable_token_limit(error_body)
            can_retry = (
                exc.code == 402
                and affordable_tokens
                and affordable_tokens >= MIN_RESPONSE_MAX_TOKENS
                and affordable_tokens < active_requested_tokens
            )
            if can_retry:
                retry_tokens = max(MIN_RESPONSE_MAX_TOKENS, affordable_tokens - RETRY_OUTPUT_BUFFER)
                if retry_tokens < active_requested_tokens:
                    retry_payload = dict(active_payload)
                    retry_payload["max_tokens"] = retry_tokens
                    yield from _stream_payload(retry_payload, retry_tokens)
                    return
            raise RuntimeError(_format_provider_error(exc.code, error_body, active_requested_tokens)) from exc

    yield from _stream_payload(payload, requested_tokens)


def _yield_text_fragments(text, fragment_size=42):
    buffer = []
    current_size = 0
    for token in re.findall(r"\S+\s*", str(text or "")):
        buffer.append(token)
        current_size += len(token)
        if current_size >= fragment_size:
            yield "".join(buffer)
            buffer = []
            current_size = 0
    if buffer:
        yield "".join(buffer)


def _strip_json_fence(raw_text):
    text = str(raw_text or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def _normalize_scorecard_payload(payload):
    if not isinstance(payload, dict):
        raise RuntimeError("Structured scorecard response was not valid JSON.")

    def _clean_text(value, fallback="", limit=240):
        text = re.sub(r"\s+", " ", str(value or fallback)).strip()
        return text[:limit]

    def _clean_score(value, fallback=0):
        try:
            number = int(round(float(value)))
        except Exception:
            number = int(fallback)
        return max(0, min(100, number))

    def _clean_list(value, fallback_message):
        items = []
        seen = set()
        for item in value if isinstance(value, list) else [value]:
            clean = _clean_text(item, limit=160)
            key = clean.lower()
            if clean and key not in seen:
                seen.add(key)
                items.append(clean)
            if len(items) >= 4:
                break
        return items or [fallback_message]

    default_labels = ["Technical Depth", "Role Fit", "Evidence Strength", "Risk Profile"]
    raw_categories = payload.get("categories") if isinstance(payload.get("categories"), list) else []
    categories = []
    for index, default_label in enumerate(default_labels):
        raw_category = raw_categories[index] if index < len(raw_categories) and isinstance(raw_categories[index], dict) else {}
        signal = str(raw_category.get("signal", "neutral")).strip().lower()
        if signal not in {"positive", "neutral", "risk"}:
            signal = "neutral"
        categories.append({
            "label": _clean_text(raw_category.get("label"), fallback=default_label, limit=60),
            "score": _clean_score(raw_category.get("score"), fallback=payload.get("overall_score", 0)),
            "evidence": _clean_text(raw_category.get("evidence"), fallback="Not stated in the document.", limit=240),
            "signal": signal,
        })

    overall_score = _clean_score(
        payload.get("overall_score"),
        fallback=round(sum(category["score"] for category in categories) / max(len(categories), 1)),
    )
    confidence = str(payload.get("confidence", "medium")).strip().lower()
    if confidence not in {"low", "medium", "high"}:
        confidence = "medium"

    summary = _clean_text(payload.get("summary"), fallback="Structured fit summary generated from the active document context.", limit=420)
    headline = _clean_text(payload.get("headline"), fallback="Candidate fit scorecard ready.", limit=120)
    fit_label = _clean_text(
        payload.get("fit_label"),
        fallback="Strong fit" if overall_score >= 75 else "Moderate fit" if overall_score >= 55 else "Needs validation",
        limit=40,
    )
    strengths = _clean_list(payload.get("strengths"), "No explicit strengths were confirmed.")
    gaps = _clean_list(payload.get("gaps"), "No explicit gaps were confirmed.")
    next_step = _clean_text(
        payload.get("recommended_next_step"),
        fallback="Use this scorecard to decide whether to advance, validate, or hold the candidate.",
        limit=220,
    )
    document_type = _clean_text(payload.get("document_type"), fallback="candidate_document", limit=40)
    markdown_summary = "\n".join([
        "### SYS.UPLINK: SECURE",
        "",
        f"**Structured Fit Snapshot**: {headline}",
        "",
        f"- **Overall Score**: {overall_score}/100",
        f"- **Fit Label**: {fit_label}",
        f"- **Confidence**: {confidence.title()}",
        f"- **Recommendation**: {next_step}",
        "",
        f"**Summary**: {summary}",
    ]).strip()

    return {
        "document_type": document_type,
        "headline": headline,
        "summary": summary,
        "overall_score": overall_score,
        "confidence": confidence,
        "fit_label": fit_label,
        "categories": categories,
        "strengths": strengths,
        "gaps": gaps,
        "recommended_next_step": next_step,
        "markdown_summary": markdown_summary,
    }


def _query_likely_needs_tooling(query):
    prompt = str(query or "").lower()
    tool_terms = (
        "linkedin",
        "github",
        "portfolio",
        "osint",
        "web presence",
        "public profile",
        "stock price",
        "share price",
        "ticker",
        "bitcoin",
        "btc",
        "crypto",
        "commodity",
        "gold",
        "silver",
        "oil",
        "current market",
    )
    return any(term in prompt for term in tool_terms)


def run_openrouter_chat(messages, temperature=0.2, max_tokens=None):
    active_api_key = str(HARDCODED_API_KEY or "").strip()
    if not active_api_key:
        raise RuntimeError("OpenRouter is not configured on the server.")

    if not isinstance(messages, list) or not messages:
        raise RuntimeError("No messages were provided to the AI proxy.")

    result = _request_openrouter_completion(
        headers=_build_openrouter_headers(active_api_key),
        messages=messages,
        query=messages[-1].get("content", "") if isinstance(messages[-1], dict) else "",
        context_text="",
        history=None,
        temperature=temperature,
        max_tokens=max_tokens or _choose_response_token_budget(messages[-1].get("content", "") if isinstance(messages[-1], dict) else ""),
    )

    if "choices" not in result:
        raise RuntimeError(f"Error in API response: {result}")

    content = _extract_message_text(result["choices"][0].get("message", {}))
    if content:
        return content
    raise RuntimeError("The AI proxy returned an empty response.")

def stream_financial_answer(query, context_text, api_key_ignored=None, history=None, interview_mode=False, analysis_mode="financial"):
    active_api_key = str(HARDCODED_API_KEY or "").strip()
    if not active_api_key:
        raise RuntimeError("OpenRouter is not configured on the server.")

    context_mode = _infer_document_analysis_mode(context_text)
    normalized_mode = "resume" if str(analysis_mode or "").strip().lower() == "resume" else "financial"
    if normalized_mode == "resume" and context_mode == "financial":
        yield {"type": "delta", "content": _mode_restriction_message("resume")}
        return
    if normalized_mode == "financial" and context_mode != "financial":
        yield {"type": "delta", "content": _mode_restriction_message("financial")}
        return

    messages, effective_query, optimized_context_text = _build_document_chat_messages(
        query,
        context_text,
        history=history,
        interview_mode=interview_mode,
        analysis_mode=normalized_mode,
    )
    headers = _build_openrouter_headers(active_api_key)
    response_budget = _choose_response_token_budget(effective_query, optimized_context_text, history)

    try:
        for chunk in _request_openrouter_stream(
            headers=headers,
            messages=messages,
            query=effective_query,
            context_text=optimized_context_text,
            history=history,
            temperature=0.4,
            max_tokens=response_budget,
        ):
            yield {"type": "delta", "content": chunk}
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"> **Unexpected Error**: {str(exc)}") from exc


def get_candidate_scorecard(query, context_text, history=None, interview_mode=False, analysis_mode="resume"):
    active_api_key = str(HARDCODED_API_KEY or "").strip()
    if not active_api_key:
        raise RuntimeError("OpenRouter is not configured on the server.")
    if str(analysis_mode or "").strip().lower() != "resume":
        raise RuntimeError("Structured scorecards are only available in Resume Analysis.")
    if _infer_document_analysis_mode(context_text) == "financial":
        raise RuntimeError(_mode_restriction_message("resume"))

    optimized_context_text = str(context_text or "")[:MAX_CONTEXT_CHARS]
    messages = [{
        "role": "system",
        "content": (
            "You are generating a structured fit scorecard from the active document.\n"
            "Output ONLY raw valid JSON. No markdown fences. No prose outside JSON.\n"
            "Use only evidence explicitly supported by the document context.\n"
            "If a detail is missing, say 'Not stated in the document.' inside the relevant field.\n"
            "Schema:\n"
            "{"
            "\"document_type\":\"resume|job_description|financial|general\","
            "\"headline\":\"short title\","
            "\"summary\":\"2-3 sentences\","
            "\"overall_score\":0,"
            "\"confidence\":\"low|medium|high\","
            "\"fit_label\":\"string\","
            "\"categories\":["
            "{\"label\":\"Technical Depth\",\"score\":0,\"evidence\":\"string\",\"signal\":\"positive|neutral|risk\"},"
            "{\"label\":\"Role Fit\",\"score\":0,\"evidence\":\"string\",\"signal\":\"positive|neutral|risk\"},"
            "{\"label\":\"Evidence Strength\",\"score\":0,\"evidence\":\"string\",\"signal\":\"positive|neutral|risk\"},"
            "{\"label\":\"Risk Profile\",\"score\":0,\"evidence\":\"string\",\"signal\":\"positive|neutral|risk\"}"
            "],"
            "\"strengths\":[\"string\"],"
            "\"gaps\":[\"string\"],"
            "\"recommended_next_step\":\"string\""
            "}\n"
            + ("Interviewer lens is active. Bias the scorecard toward claim validation and technical depth.\n" if interview_mode else "")
            + f"\n--- DOCUMENT CONTEXT ---\n{optimized_context_text}\n--- END OF DOCUMENT CONTEXT ---"
        ),
    }]
    messages.extend(_normalize_history_messages(history))
    messages.append({
        "role": "user",
        "content": str(query or "").strip() or "Evaluate the candidate fit and produce a structured scorecard.",
    })

    result = _request_openrouter_completion(
        headers=_build_openrouter_headers(active_api_key),
        messages=messages,
        query=_extract_message_text(messages[-1]),
        context_text=optimized_context_text,
        history=history,
        temperature=0.2,
        max_tokens=700,
    )
    if "choices" not in result:
        raise RuntimeError(f"Error in API response: {result}")

    raw_content = _extract_message_text(result["choices"][0].get("message", {}))
    if not raw_content:
        raise RuntimeError("The AI proxy returned an empty scorecard.")

    try:
        payload = json.loads(_strip_json_fence(raw_content))
    except Exception as exc:
        raise RuntimeError("The AI proxy returned invalid scorecard JSON.") from exc
    return _normalize_scorecard_payload(payload)


def get_financial_answer(query, context_text, api_key_ignored=None, history=None, interview_mode=False, analysis_mode="financial"):
    active_api_key = str(HARDCODED_API_KEY or "").strip()
    if not active_api_key:
        return "OpenRouter is not configured on the server."

    normalized_mode = "resume" if str(analysis_mode or "").strip().lower() == "resume" else "financial"
    context_mode = _infer_document_analysis_mode(context_text)
    if normalized_mode == "resume" and context_mode == "financial":
        return _mode_restriction_message("resume")
    if normalized_mode == "financial" and context_mode != "financial":
        return _mode_restriction_message("financial")

    messages, effective_query, optimized_context_text = _build_document_chat_messages(
        query,
        context_text,
        history=history,
        interview_mode=interview_mode,
        analysis_mode=normalized_mode,
    )

    try:
        result = _request_openrouter_completion(
            headers=_build_openrouter_headers(active_api_key),
            messages=messages,
            query=effective_query,
            context_text=optimized_context_text,
            history=history,
            temperature=0.4,
            max_tokens=_choose_response_token_budget(effective_query, optimized_context_text, history),
        )
        if "choices" not in result:
            return f"Error in API response: {result}"

        message = result["choices"][0].get("message", {})
        return _extract_message_text(message)
    except RuntimeError as exc:
        return str(exc)
    except Exception as exc:
        return f"> **Unexpected Error**: {str(exc)}"
