"""Fetchers + normalizers for the official public job-board APIs of Greenhouse, Lever and Ashby.

Each normalizer returns one flat, unified dict per job. Recruiter / hiring-manager names and
e-mails are never output (none of the posting APIs expose them as fields; e-mail addresses
inside description text are redacted).
"""
import asyncio
import html
import logging
import re
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import parse_qs, urlparse

import httpx

UA = "ats-jobs-mcp/1.0 (MCP server; reads public job-board APIs; python-httpx)"
ATS = ("greenhouse", "lever", "ashby")
# Minimum seconds between requests to the same API host (api.lever.co asks for Crawl-delay: 1).
LEVER_MAX_SKIP = 2000  # max 2,100 postings per Lever board per call
MIN_INTERVAL = {"greenhouse": 0.5, "lever": 1.0, "lever-eu": 1.0, "ashby": 0.5}

EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")


class NotFound(Exception):
    pass


class FetchError(Exception):
    pass


# ---------------------------------------------------------------- HTML -> text
class _Text(HTMLParser):
    BLOCK = {"p", "div", "br", "li", "ul", "ol", "h1", "h2", "h3", "h4", "h5", "h6", "tr", "section", "blockquote"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "li":
            self.out.append("\n- ")
        elif tag in self.BLOCK:
            self.out.append("\n")

    def handle_endtag(self, tag):
        if tag in self.BLOCK:
            self.out.append("\n")

    def handle_data(self, data):
        self.out.append(data)


def html_to_text(s: str | None) -> str | None:
    if not s:
        return None
    if "&lt;" in s and "<" not in s:  # Greenhouse returns HTML-escaped HTML
        s = html.unescape(s)
    p = _Text()
    p.feed(s)
    p.close()
    t = "".join(p.out).replace("\xa0", " ")
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r" *\n *", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    return redact(t) or None


def redact(t: str | None) -> str | None:
    return EMAIL_RE.sub("[email removed]", t) if t else t


# ---------------------------------------------------------------- input parsing
def parse_careers_url(url: str) -> tuple[str, str] | None:
    """Return (ats, slug) for a known ATS board URL, else None."""
    url = (url or "").strip()
    if not url:
        return None
    if "://" not in url:
        url = "https://" + url
    u = urlparse(url)
    host = (u.hostname or "").lower()
    parts = [p for p in u.path.split("/") if p]
    q = parse_qs(u.query)
    if host.endswith("greenhouse.io"):
        if q.get("for"):  # boards.greenhouse.io/embed/job_board?for=slug
            return "greenhouse", q["for"][0]
        if host.startswith("boards-api") and len(parts) >= 3 and parts[:2] == ["v1", "boards"]:
            return "greenhouse", parts[2]
        if parts and parts[0] not in ("embed",):
            return "greenhouse", parts[0]
    if host in ("jobs.lever.co", "api.lever.co") and parts:
        return "lever", parts[-1] if host == "api.lever.co" else parts[0]
    if host in ("jobs.eu.lever.co", "api.eu.lever.co") and parts:
        return "lever-eu", parts[-1] if host == "api.eu.lever.co" else parts[0]
    if host == "jobs.ashbyhq.com" and parts:
        return "ashby", parts[0]
    if host == "api.ashbyhq.com" and parts:
        return "ashby", parts[-1]
    return None


# ---------------------------------------------------------------- HTTP
class Client:
    def __init__(self, log=None):
        self.log = log or logging.getLogger("ats")
        self.http = httpx.AsyncClient(headers={"User-Agent": UA, "Accept": "application/json"},
                                      follow_redirects=True, timeout=httpx.Timeout(30, connect=10))
        self._locks = {k: asyncio.Lock() for k in MIN_INTERVAL}
        self._last = {k: 0.0 for k in MIN_INTERVAL}
        self.requests = 0

    async def close(self):
        await self.http.aclose()

    async def get_json(self, host_key: str, url: str, params: dict | None = None):
        last_err = ""
        for attempt in range(3):
            async with self._locks[host_key]:
                wait = MIN_INTERVAL[host_key] - (time.monotonic() - self._last[host_key])
                if wait > 0:
                    await asyncio.sleep(wait)
                self._last[host_key] = time.monotonic()
                self.requests += 1
                try:
                    r = await self.http.get(url, params=params)
                except httpx.HTTPError as e:
                    r, last_err = None, f"{type(e).__name__}: {e}"
            if r is not None:
                if r.status_code == 200:
                    try:
                        return r.json()
                    except ValueError:
                        last_err = "invalid JSON"
                elif r.status_code == 404:
                    raise NotFound(url)
                elif r.status_code in (429,) or r.status_code >= 500:
                    last_err = f"HTTP {r.status_code}"
                    ra = r.headers.get("Retry-After")
                    if ra and ra.isdigit():
                        await asyncio.sleep(min(int(ra), 10))
                else:
                    raise FetchError(f"HTTP {r.status_code}")
            self.log.warning(f"{url} attempt {attempt + 1}/3 failed: {last_err}")
            if attempt < 2:
                await asyncio.sleep(1.5 * 2 ** attempt)
        raise FetchError(last_err or "failed")

    # -- raw fetchers ---------------------------------------------------------
    async def greenhouse(self, slug: str, content: bool) -> list[dict]:
        # content=true is always requested: departments and offices are only returned with it.
        params = {"pay_transparency": "true", "content": "true"}
        d = await self.get_json("greenhouse", f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs", params)
        return d.get("jobs") or []

    async def lever(self, slug: str, eu: bool = False) -> list[dict]:
        key = "lever-eu" if eu else "lever"
        base = f"https://api.{'eu.' if eu else ''}lever.co/v0/postings/{slug}"
        out, skip, limit = [], 0, 100
        while True:
            d = await self.get_json(key, base, {"mode": "json", "skip": skip, "limit": limit})
            if isinstance(d, dict):  # {"ok": false, "error": "Document not found"}
                raise NotFound(base)
            out.extend(d)
            if len(d) < limit or skip >= LEVER_MAX_SKIP:
                return out
            skip += limit

    async def ashby(self, slug: str) -> list[dict]:
        d = await self.get_json("ashby", f"https://api.ashbyhq.com/posting-api/job-board/{slug}",
                                {"includeCompensation": "true"})
        return d.get("jobs") or []

    # -- single-posting fetchers ---------------------------------------------
    async def greenhouse_job(self, slug: str, job_id: str) -> dict:
        return await self.get_json("greenhouse", f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{job_id}",
                                   {"pay_transparency": "true"})

    async def lever_job(self, slug: str, job_id: str, eu: bool = False) -> dict:
        key = "lever-eu" if eu else "lever"
        d = await self.get_json(key, f"https://api.{'eu.' if eu else ''}lever.co/v0/postings/{slug}/{job_id}", {"mode": "json"})
        if not isinstance(d, dict) or d.get("ok") is False:
            raise NotFound(job_id)
        return d


# ---------------------------------------------------------------- normalizers
def _iso(v) -> str | None:
    if v in (None, ""):
        return None
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError:
        return str(v)


def _workplace(v: str | None, *texts) -> str | None:
    v = (v or "").lower().replace("-", "").replace("_", "")
    if v in ("remote", "hybrid", "onsite"):
        return v
    blob = " ".join(t or "" for t in texts).lower()
    if "hybrid" in blob:
        return "hybrid"
    if "remote" in blob:
        return "remote"
    return None


def _emp(v: str | None) -> str | None:
    if not v:
        return None
    k = re.sub(r"[^a-z]", "", v.lower())
    m = {"fulltime": "full-time", "permanent": "full-time", "regular": "full-time", "parttime": "part-time",
         "contract": "contract", "contractor": "contract", "temporary": "temporary", "temp": "temporary",
         "intern": "internship", "internship": "internship"}
    return m.get(k, v)


def _base(ats, slug) -> dict:
    return {"ats": ats, "company": slug, "companySlug": slug}


def norm_greenhouse(j: dict, slug: str, include_desc: bool) -> dict:
    depts = [d.get("name") for d in j.get("departments") or [] if d.get("name")]
    offices = [o.get("name") for o in j.get("offices") or [] if o.get("name")]
    loc = (j.get("location") or {}).get("name")
    pay = (j.get("pay_input_ranges") or [None])[0]
    meta = {}
    for m in j.get("metadata") or []:
        if isinstance(m, dict) and m.get("name") and isinstance(m.get("value"), str):
            meta[m["name"].lower()] = m["value"]
    emp = next((v for k, v in meta.items() if "employment" in k or "time type" in k), None)
    wp = next((v for k, v in meta.items() if "workplace" in k or "remote" in k or "location type" in k), None)
    out = _base("greenhouse", slug)
    out.update({
        "company": j.get("company_name") or slug,
        "jobId": str(j.get("id")),
        "title": (j.get("title") or "").strip() or None,
        "department": depts[0] if depts else None,
        "team": depts[1] if len(depts) > 1 else None,
        "location": loc,
        "allLocations": list(dict.fromkeys(x for x in [loc] + offices if x)) or None,
        "country": None,
        "workplaceType": _workplace(None, wp, loc),
        "employmentType": _emp(emp),
        "compensationText": None,
        "salaryMin": None, "salaryMax": None, "salaryCurrency": None, "salaryInterval": None,
        "postedAt": _iso(j.get("first_published")) or _iso(j.get("updated_at")),
        "updatedAt": _iso(j.get("updated_at")),
        "applyUrl": j.get("absolute_url"),
        "url": j.get("absolute_url"),
        "requisitionId": j.get("requisition_id"),
        "language": j.get("language"),
    })
    if pay:
        mn, mx = pay.get("min_cents"), pay.get("max_cents")
        out["salaryMin"] = mn / 100 if isinstance(mn, (int, float)) else None
        out["salaryMax"] = mx / 100 if isinstance(mx, (int, float)) else None
        out["salaryCurrency"] = pay.get("currency_type")
        t = (pay.get("title") or "").lower()
        out["salaryInterval"] = "hour" if "hour" in t else ("year" if (out["salaryMin"] or 0) >= 1000 else None)
        rng = f"{out['salaryMin']:,.0f}-{out['salaryMax']:,.0f} {out['salaryCurrency'] or ''}".strip() if out["salaryMin"] is not None and out["salaryMax"] is not None else ""
        out["compensationText"] = " ".join(x for x in [(pay.get("title") or "").strip(" :"), rng] if x) or None
    if include_desc:
        out["descriptionText"] = html_to_text(j.get("content"))
    return out


def norm_lever(j: dict, slug: str, include_desc: bool, eu: bool = False) -> dict:
    c = j.get("categories") or {}
    sal = j.get("salaryRange") or {}
    out = _base("lever", slug)
    out.update({
        "jobId": j.get("id"),
        "title": (j.get("text") or "").strip() or None,
        "department": c.get("department"),
        "team": c.get("team"),
        "location": c.get("location"),
        "allLocations": c.get("allLocations") or ([c["location"]] if c.get("location") else None),
        "country": j.get("country"),
        "workplaceType": _workplace(j.get("workplaceType"), c.get("location")),
        "employmentType": _emp(c.get("commitment")),
        "compensationText": None,
        "salaryMin": sal.get("min"), "salaryMax": sal.get("max"),
        "salaryCurrency": sal.get("currency"),
        "salaryInterval": (sal.get("interval") or "").replace("per-", "").replace("-", " ") or None,
        "postedAt": _iso(j.get("createdAt")),
        "updatedAt": None,
        "applyUrl": j.get("applyUrl"),
        "url": j.get("hostedUrl"),
        "requisitionId": None,
        "language": None,
    })
    if sal.get("min") is not None:
        out["compensationText"] = f"{sal.get('min'):,}-{sal.get('max'):,} {sal.get('currency') or ''} {out['salaryInterval'] or ''}".strip()
    if include_desc:
        parts = [j.get("descriptionPlain") or html_to_text(j.get("description"))]
        for lst in j.get("lists") or []:
            body = html_to_text(lst.get("content"))
            parts.append(f"{lst.get('text') or ''}\n{body or ''}".strip())
        parts.append(j.get("additionalPlain") or html_to_text(j.get("additional")))
        out["descriptionText"] = redact("\n\n".join(p.strip() for p in parts if p and p.strip())) or None
    return out


def norm_ashby(j: dict, slug: str, include_desc: bool) -> dict:
    comp = j.get("compensation") or {}
    sal = next((c for c in comp.get("summaryComponents") or [] if c.get("compensationType") == "Salary"), {}) or {}
    addr = ((j.get("address") or {}).get("postalAddress") or {})
    sec = [s.get("location") for s in j.get("secondaryLocations") or [] if isinstance(s, dict) and s.get("location")]
    interval = (sal.get("interval") or "").lower()
    out = _base("ashby", slug)
    out.update({
        "jobId": j.get("id"),
        "title": (j.get("title") or "").strip() or None,
        "department": j.get("department"),
        "team": j.get("team"),
        "location": j.get("location"),
        "allLocations": list(dict.fromkeys(x for x in [j.get("location")] + sec if x)) or None,
        "country": addr.get("addressCountry"),
        "workplaceType": _workplace(j.get("workplaceType"), "remote" if j.get("isRemote") else ""),
        "employmentType": _emp(j.get("employmentType")),
        "compensationText": comp.get("compensationTierSummary") or comp.get("scrapeableCompensationSalarySummary"),
        "salaryMin": sal.get("minValue"), "salaryMax": sal.get("maxValue"),
        "salaryCurrency": sal.get("currencyCode"),
        "salaryInterval": {"1 year": "year", "1 month": "month", "1 week": "week", "1 day": "day", "1 hour": "hour"}.get(interval, interval or None),
        "postedAt": _iso(j.get("publishedAt")),
        "updatedAt": None,
        "applyUrl": j.get("applyUrl"),
        "url": j.get("jobUrl"),
        "requisitionId": None,
        "language": None,
    })
    if include_desc:
        out["descriptionText"] = redact(j.get("descriptionPlain")) or html_to_text(j.get("descriptionHtml"))
    return out
