"""ATS Jobs MCP server: open jobs from Greenhouse, Lever and Ashby official public job-board APIs.

Read-only, public, no-auth endpoints only. Job-posting data only: no recruiter names, e-mails or phones
(e-mail addresses inside descriptions are redacted). Unlisted Ashby postings are skipped.
"""
import asyncio
import logging
import os
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import httpx
from mcp.server.fastmcp import FastMCP

sys.path.insert(0, os.path.dirname(__file__))
from ats import UA, Client, FetchError, NotFound, norm_ashby, norm_greenhouse, norm_lever, parse_careers_url  # noqa: E402
from jobs import fetch_company, matches, parse_job_url, parse_target, terms  # noqa: E402

logging.getLogger("httpx").setLevel(logging.WARNING)

mcp = FastMCP("ats-jobs", host=os.environ.get("HOST", "0.0.0.0"), port=int(os.environ.get("PORT", "8080")))

MAX_COMPANIES = 20        # companies per search_jobs call
MAX_RESULTS = 200         # jobs returned per call
DESC_CHARS_SEARCH = 3000  # descriptionText cap in search_jobs
DESC_CHARS_FULL = 20000   # descriptionText cap in get_job
CALL_TIMEOUT = 120        # seconds per tool call
PAGE_MAX_BYTES = 2_000_000

# Fields kept in compact list output (descriptions excluded).
LIST_FIELDS = ("ats", "company", "companySlug", "jobId", "title", "department", "team", "location", "workplaceType",
               "employmentType", "compensationText", "salaryMin", "salaryMax", "salaryCurrency", "salaryInterval",
               "postedAt", "url")


def _cut(t: str | None, n: int) -> str | None:
    return t if not t or len(t) <= n else t[:n].rstrip() + f"… [truncated, {len(t)} chars total]"


def _finish(job: dict, desc_chars: int | None) -> dict:
    job["isRemote"] = job.get("workplaceType") == "remote"
    if "descriptionText" in job and desc_chars:
        job["descriptionText"] = _cut(job["descriptionText"], desc_chars)
    return job


async def _with_client(coro_fn):
    client = Client()
    try:
        return await asyncio.wait_for(coro_fn(client), timeout=CALL_TIMEOUT)
    except asyncio.TimeoutError:
        return {"error": f"timed out after {CALL_TIMEOUT}s"}
    finally:
        await client.close()


@mcp.tool()
async def search_jobs(companies: list[str], keywords: list[str] | None = None, locations: list[str] | None = None,
                      departments: list[str] | None = None, remote_only: bool = False,
                      posted_within_days: int | None = None, include_description: bool = False,
                      limit: int = 50, max_per_company: int | None = None) -> dict:
    """Search open jobs across up to 20 companies that hire via Greenhouse, Lever or Ashby.

    companies: each item is one of
      - a board slug, e.g. "airbnb" (ATS auto-detected; if the slug exists on several ATS, the board with
        the most open jobs is used),
      - "ats:slug", e.g. "greenhouse:airbnb", "lever:spotify", "ashby:ramp", "lever-eu:<slug>",
      - a board URL, e.g. "https://jobs.ashbyhq.com/ramp" or "boards.greenhouse.io/airbnb".
      The slug is NOT always the company name; use detect_ats if unsure.
    Filters (all case-insensitive substring matches, ANY term matches):
      keywords -> title/department/team; locations -> location/allLocations/country (e.g. "London", "Remote");
      departments -> department/team; remote_only -> workplaceType == "remote";
      posted_within_days -> postedAt (Greenhouse: first_published, else updated_at) within N days.
    include_description: add descriptionText (plain text, e-mails redacted, capped at 3,000 chars; use get_job for full text).
    limit: max jobs returned in total (1-200, default 50); max_per_company: optional per-company cap.
    Returns {"jobs": [...newest first per company...], "companies": [{ats, slug, status, jobsFound, matched, returned, error}],
             "returned": n, "totalMatched": n}.
    Job fields: ats, company, companySlug, jobId, title, department, team, location, allLocations, country,
    workplaceType (remote/hybrid/onsite/null), isRemote, employmentType, compensationText, salaryMin, salaryMax,
    salaryCurrency, salaryInterval, postedAt, updatedAt, applyUrl, url, requisitionId, language. null = not published.
    """
    limit = max(1, min(int(limit or 50), MAX_RESULTS))
    targets, bad = [], []
    for c in (companies or [])[:MAX_COMPANIES]:
        t = parse_target(c)
        (targets.append(t) if t else bad.append(c))
    targets = list(dict.fromkeys(targets))
    if not targets:
        return {"error": "no valid companies given", "invalid": bad, "jobs": []}
    kw, loc, dp = terms(keywords), terms(locations), terms(departments)
    since = datetime.now(timezone.utc) - timedelta(days=int(posted_within_days)) if posted_within_days else None

    async def run(client: Client):
        sem = asyncio.Semaphore(6)

        async def one(ats, slug):
            row = {"ats": ats, "slug": slug, "status": "ok", "jobsFound": 0, "matched": 0, "returned": 0, "error": None}
            async with sem:
                try:
                    resolved, jobs = await fetch_company(client, ats, slug, include_description)
                except NotFound:
                    row.update(status="not found", error="board not found (check slug / ATS, or use detect_ats)")
                    return row, []
                except Exception as e:  # noqa: BLE001 - per-company isolation
                    row.update(status="error", error=f"{type(e).__name__}: {str(e)[:200]}")
                    return row, []
            row["ats"] = resolved
            row["jobsFound"] = len(jobs)
            sel = [j for j in jobs if matches(j, kw, loc, dp, remote_only, since)]
            sel.sort(key=lambda j: j.get("postedAt") or "", reverse=True)
            row["matched"] = len(sel)
            if max_per_company:
                sel = sel[:int(max_per_company)]
            return row, sel

        results = await asyncio.gather(*(one(a, s) for a, s in targets))
        out, summary = [], []
        for row, sel in results:  # fill in input order, respecting the global limit
            take = sel[:max(limit - len(out), 0)]
            row["returned"] = len(take)
            out.extend(_finish(j, DESC_CHARS_SEARCH) for j in take)
            summary.append(row)
        res = {"jobs": out, "companies": summary, "returned": len(out), "totalMatched": sum(r["matched"] for r in summary)}
        if bad:
            res["invalid"] = bad
        if len(companies or []) > MAX_COMPANIES:
            res["note"] = f"only the first {MAX_COMPANIES} companies were searched"
        return res

    return await _with_client(run)


@mcp.tool()
async def list_company_jobs(company: str, ats: str = "auto", keyword: str | None = None, limit: int = 100) -> dict:
    """List all open jobs of one company (compact: no descriptions), newest first, plus department counts.

    company: board slug ("airbnb"), "ats:slug" or a board URL ("https://jobs.lever.co/spotify").
    ats: "greenhouse", "lever", "lever-eu", "ashby" or "auto" (default: board with the most jobs; ignored if company
    already names the ATS).
    keyword: optional comma-separated title/department/team filter (any term).
    limit: max jobs returned (1-200, default 100). totalJobs always reflects the full board.
    Returns {ats, slug, totalJobs, matched, returned, departments: {name: count}, jobs: [{jobId, title, department,
    team, location, workplaceType, employmentType, compensationText, salaryMin/Max/Currency/Interval, postedAt, url}]}.
    Use get_job(ats, slug, jobId) for the full description.
    """
    t = parse_target(company)
    if not t:
        return {"error": f"cannot parse company '{company}'"}
    a, slug = t
    if a == "auto" and ats and ats.lower() != "auto":
        a = ats.lower()
    limit = max(1, min(int(limit or 100), MAX_RESULTS))

    async def run(client):
        try:
            resolved, jobs = await fetch_company(client, a, slug, False)
        except NotFound:
            return {"error": f"board '{slug}' not found on {a if a != 'auto' else 'Greenhouse, Lever or Ashby'}"}
        except FetchError as e:
            return {"error": str(e)}
        jobs.sort(key=lambda j: j.get("postedAt") or "", reverse=True)
        kw = terms(keyword)
        sel = [j for j in jobs if matches(j, kw, [], [], False, None)]
        depts = Counter(j.get("department") or "(none)" for j in jobs)
        return {"ats": resolved, "slug": slug, "company": jobs[0]["company"] if jobs else slug, "totalJobs": len(jobs),
                "matched": len(sel), "returned": min(len(sel), limit), "departments": dict(depts.most_common(50)),
                "jobs": [{k: j.get(k) for k in LIST_FIELDS} for j in sel[:limit]]}

    return await _with_client(run)


@mcp.tool()
async def get_job(ats: str | None = None, company: str | None = None, job_id: str | None = None,
                  job_url: str | None = None) -> dict:
    """Full details of one job posting, including the complete description as plain text.

    Either pass job_url (e.g. "https://jobs.lever.co/spotify/<uuid>", "https://job-boards.greenhouse.io/airbnb/jobs/123",
    "https://jobs.ashbyhq.com/ramp/<uuid>") or all of ats ("greenhouse"|"lever"|"lever-eu"|"ashby"), company (board slug)
    and job_id (jobId from search_jobs/list_company_jobs).
    Returns the unified job record (see search_jobs) plus descriptionText (e-mails redacted, capped at 20,000 chars).
    Returns {"error": ...} if the posting is closed/unknown.
    """
    if job_url:
        p = parse_job_url(job_url)
        if not p:
            return {"error": "unsupported job URL; expected a Greenhouse, Lever or Ashby job-detail URL"}
        ats, company, job_id = p
    if not (ats and company and job_id):
        return {"error": "pass job_url, or ats + company + job_id"}
    ats, company, job_id = ats.lower().strip(), company.strip().strip("/"), str(job_id).strip()

    async def run(client):
        try:
            if ats == "greenhouse":
                j = await client.greenhouse_job(company, job_id)
                rec = norm_greenhouse(j, company, True)
            elif ats in ("lever", "lever-eu"):
                rec = norm_lever(await client.lever_job(company, job_id, eu=ats == "lever-eu"), company, True)
            elif ats == "ashby":  # Ashby has no single-posting endpoint: read the board and pick the job
                raw = await client.ashby(company)
                j = next((x for x in raw if x.get("id") == job_id), None)
                if not j:
                    return {"error": f"job {job_id} not found on ashby board '{company}' (closed or wrong id)"}
                rec = norm_ashby(j, company, True)
            else:
                return {"error": f"unknown ats '{ats}'"}
        except NotFound:
            return {"error": f"job {job_id} not found on {ats} board '{company}' (closed or wrong id)"}
        except FetchError as e:
            return {"error": str(e)}
        return _finish(rec, DESC_CHARS_FULL)

    return await _with_client(run)


_ATS_LINK = re.compile(
    r"https?://(?:boards|job-boards|boards-api)(?:\.eu)?\.greenhouse\.io/[^\s\"'<>)]+"
    r"|https?://jobs(?:\.eu)?\.lever\.co/[^\s\"'<>)]+"
    r"|https?://jobs\.ashbyhq\.com/[^\s\"'<>)]+", re.I)
_GH_EMBED = re.compile(r"greenhouse\.io/embed/job_board(?:/js)?\?for=([\w-]+)", re.I)


async def _scan_page(http: httpx.AsyncClient, url: str) -> tuple[list[tuple[str, str]], str | None]:
    """Fetch one public page (max 2 MB) and return ATS boards linked/embedded in it."""
    try:
        async with http.stream("GET", url) as r:
            if r.status_code != 200:
                return [], f"HTTP {r.status_code}"
            buf = b""
            async for chunk in r.aiter_bytes():
                buf += chunk
                if len(buf) > PAGE_MAX_BYTES:
                    break
            final = str(r.url)
    except httpx.HTTPError as e:
        return [], f"{type(e).__name__}"
    text = buf.decode("utf-8", "ignore")
    found = [parse_careers_url(final)] + [parse_careers_url(m) for m in _ATS_LINK.findall(text)]
    found += [("greenhouse", s) for s in _GH_EMBED.findall(text)]
    found = [f for f in found if f and f[1] not in ("embed", "v1", "jobs")]
    return list(dict.fromkeys(found)), None


async def _probe(client: Client, ats: str, slug: str) -> dict | None:
    """Cheap existence check of a board; returns {ats, slug, boardName, openJobs?} or None."""
    try:
        if ats == "greenhouse":
            d = await client.get_json("greenhouse", f"https://boards-api.greenhouse.io/v1/boards/{slug}")
            jobs = await client.get_json("greenhouse", f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
            return {"ats": ats, "slug": slug, "boardName": d.get("name"), "openJobs": len(jobs.get("jobs") or [])}
        if ats in ("lever", "lever-eu"):
            eu = ats == "lever-eu"
            d = await client.get_json(ats, f"https://api.{'eu.' if eu else ''}lever.co/v0/postings/{slug}",
                                      {"mode": "json", "limit": 1})
            if isinstance(d, dict):
                return None
            return {"ats": ats, "slug": slug, "boardName": slug, "openJobs": None if d else 0,
                    "note": "Lever: openJobs not counted by probe" if d else None}
        if ats == "ashby":
            d = await client.get_json("ashby", f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
            return {"ats": ats, "slug": slug, "boardName": slug,
                    "openJobs": sum(1 for j in d.get("jobs") or [] if j.get("isListed", True))}
    except (NotFound, FetchError):
        return None
    return None


def _slug_candidates(q: str) -> list[str]:
    q = q.strip().lower()
    host = q.split("://")[-1].split("/")[0]
    if "." in host:  # domain: stripe.com, careers.stripe.com -> stripe
        labels = [x for x in host.split(".") if x not in ("www", "careers", "jobs", "com", "io", "co", "ai", "org", "net", "app", "dev", "uk", "de")]
        base = labels[-1] if labels else host.split(".")[0]
    else:
        base = q
    words = re.findall(r"[a-z0-9]+", base)
    c = ["".join(words), "-".join(words)]
    return [x for x in dict.fromkeys(c) if x][:2]


@mcp.tool()
async def detect_ats(query: str) -> dict:
    """Find which ATS (Greenhouse, Lever, Ashby) a company uses and its board slug.

    query: a company name ("Ramp"), a company domain ("ramp.com"), a careers page URL ("https://www.ramp.com/careers")
    or an ATS board/job URL ("https://jobs.lever.co/spotify/...").
    Method: (1) ATS URLs are parsed directly; (2) for a domain/URL, the page (and for a bare domain also /careers) is
    fetched once and scanned for linked/embedded Greenhouse/Lever/Ashby boards; (3) slug guesses derived from the
    name/domain are probed against the three public APIs.
    Returns {"query", "boards": [{ats, slug, boardName, openJobs, foundVia}], "checked": [...]}. An empty boards list
    means none of the three ATS were found (the company may use Workday, SmartRecruiters, its own site, etc.).
    Guessed slugs can belong to a different company with the same name; check boardName / a job URL.
    """
    q = (query or "").strip()
    if not q:
        return {"error": "empty query"}

    async def run(client: Client):
        found, checked, seen = [], [], set()

        async def add(ats, slug, via):
            key = ("lever" if ats == "lever-eu" else ats, slug.lower())
            if key in seen:
                return
            seen.add(key)
            r = await _probe(client, ats, slug)
            checked.append(f"{ats}:{slug} ({via}) -> {'found' if r else 'no'}")
            if r:
                r["foundVia"] = via
                found.append(r)

        direct = parse_careers_url(q) if ("/" in q or "." in q) else None
        if direct:
            await add(*direct, "url")
            return {"query": q, "boards": found, "checked": checked}
        is_domain = "." in q.split("://")[-1].split("/")[0] and " " not in q
        if is_domain:
            base = q if "://" in q else "https://" + q
            pages = [base] + ([base.rstrip("/") + "/careers"] if urlparse(base).path in ("", "/") else [])
            async with httpx.AsyncClient(headers={"User-Agent": UA}, follow_redirects=True, timeout=15) as http:
                for p in pages:
                    links, err = await _scan_page(http, p)
                    checked.append(f"page {p} -> {err or f'{len(links)} ATS link(s)'}")
                    for ats, slug in links[:5]:
                        await add(ats, slug, f"link on {p}")
        if not found:
            for slug in _slug_candidates(q):
                for ats in ("greenhouse", "lever", "ashby"):
                    await add(ats, slug, "slug guess")
        found.sort(key=lambda b: -(b.get("openJobs") or 0))
        return {"query": q, "boards": found, "checked": checked}

    return await _with_client(run)


from starlette.requests import Request  # noqa: E402
from starlette.responses import JSONResponse  # noqa: E402


@mcp.custom_route("/health", methods=["GET"])
async def health(_: Request) -> JSONResponse:
    return JSONResponse({"status": "healthy"})


if __name__ == "__main__":
    mcp.run(transport=os.environ.get("MCP_TRANSPORT", "streamable-http"))
