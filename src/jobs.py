"""Company fetching + filtering, ported from the ATS Jobs API actor (src/main.py), without Apify."""
import asyncio
import re
from datetime import datetime
from urllib.parse import parse_qs, urlparse

from ats import (Client, FetchError, NotFound, norm_ashby, norm_greenhouse,
                 norm_lever, parse_careers_url)

AUTO_ORDER = ("greenhouse", "lever", "ashby", "lever-eu")
KNOWN = ("greenhouse", "lever", "lever-eu", "ashby", "auto")


def terms(v) -> list[str]:
    if not v:
        return []
    if isinstance(v, str):
        v = v.split(",")
    return [s.strip().lower() for s in v if s and str(s).strip()]


def parse_target(spec: str) -> tuple[str, str] | None:
    """'airbnb' -> (auto, airbnb); 'lever:spotify' -> (lever, spotify); board URL -> parsed; else None."""
    spec = (spec or "").strip().strip("/")
    if not spec:
        return None
    if "://" in spec or "." in spec.split("/")[0]:
        return parse_careers_url(spec)
    m = re.fullmatch(r"(greenhouse|lever-eu|lever|ashby|auto)\s*[:/]\s*([\w.-]+)", spec, re.I)
    if m:
        return m.group(1).lower(), m.group(2)
    if re.fullmatch(r"[\w.-]+", spec):
        return "auto", spec
    return None


def parse_job_url(url: str) -> tuple[str, str, str] | None:
    """Job-detail URL -> (ats, slug, job_id), or None."""
    url = (url or "").strip()
    if "://" not in url:
        url = "https://" + url
    u = urlparse(url)
    host = (u.hostname or "").lower()
    parts = [p for p in u.path.split("/") if p]
    q = parse_qs(u.query)
    if host.endswith("greenhouse.io"):
        if q.get("for") and q.get("token"):  # boards.greenhouse.io/embed/job_app?for=slug&token=123
            return "greenhouse", q["for"][0], q["token"][0]
        if len(parts) >= 3 and parts[1] == "jobs":  # (job-)boards.greenhouse.io/<slug>/jobs/<id>
            return "greenhouse", parts[0], parts[2]
    if host in ("jobs.lever.co", "jobs.eu.lever.co") and len(parts) >= 2:
        return ("lever-eu" if ".eu." in host else "lever"), parts[0], parts[1]
    if host == "jobs.ashbyhq.com" and len(parts) >= 2:
        return "ashby", parts[0], parts[1]
    return None


async def fetch_company(client: Client, ats: str, slug: str, include_desc: bool) -> tuple[str, list[dict]]:
    """Return (resolved_ats, normalized jobs). Raises NotFound / FetchError."""
    if ats == "auto":
        # Same slug can exist on several ATS (often a stale/test board): query all three hosts in
        # parallel and keep the board with the most open jobs. Lever EU only if none is found.
        res = await asyncio.gather(*(fetch_company(client, c, slug, include_desc) for c in AUTO_ORDER[:3]),
                                   return_exceptions=True)
        ok = [r for r in res if not isinstance(r, BaseException)]
        if ok:
            return max(ok, key=lambda r: len(r[1]))
        errs = [r for r in res if not isinstance(r, NotFound)]
        if errs:
            raise errs[0]
        try:
            return await fetch_company(client, "lever-eu", slug, include_desc)
        except NotFound:
            raise NotFound(f"'{slug}' not found on Greenhouse, Lever or Ashby") from None
    if ats == "greenhouse":
        raw = await client.greenhouse(slug, include_desc)
        return ats, [norm_greenhouse(j, slug, include_desc) for j in raw]
    if ats in ("lever", "lever-eu"):
        raw = await client.lever(slug, eu=ats == "lever-eu")
        return ats, [norm_lever(j, slug, include_desc) for j in raw]
    if ats == "ashby":
        raw = await client.ashby(slug)
        # isListed=false postings are "only available via direct link": respect that and skip them.
        return ats, [norm_ashby(j, slug, include_desc) for j in raw if j.get("isListed", True)]
    raise FetchError(f"unknown ats '{ats}' (use greenhouse, lever, lever-eu, ashby or auto)")


def matches(job: dict, kw: list[str], loc: list[str], depts: list[str], remote_only: bool, since) -> bool:
    if kw:
        hay = " ".join(str(job.get(k) or "") for k in ("title", "department", "team")).lower()
        if not any(k in hay for k in kw):
            return False
    if loc:
        hay = " ".join([job.get("location") or "", job.get("country") or ""] + (job.get("allLocations") or [])).lower()
        if not any(k in hay for k in loc):
            return False
    if depts:
        hay = f"{job.get('department') or ''} {job.get('team') or ''}".lower()
        if not any(k in hay for k in depts):
            return False
    if remote_only and job.get("workplaceType") != "remote":
        return False
    if since:
        ts = job.get("postedAt") or job.get("updatedAt")
        try:
            if not ts or datetime.fromisoformat(ts.replace("Z", "+00:00")) < since:
                return False
        except ValueError:
            return False
    return True
