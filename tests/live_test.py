"""Live smoke test: calls every tool against the real APIs. Run: .venv/bin/python tests/live_test.py"""
import asyncio, json, os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import server  # noqa: E402


def show(name, r, ok):
    s = json.dumps(r, ensure_ascii=False)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {len(s)} bytes | {s[:600]}\n")
    return ok


async def main():
    res = []
    tools = await server.mcp.list_tools()
    res.append(show("list_tools", [t.name for t in tools], len(tools) == 4))
    t = time.time()
    r = await server.search_jobs(["greenhouse:airbnb", "lever:spotify", "https://jobs.ashbyhq.com/ramp", "linear", "nonexistent-co-xyz"],
                                 keywords=["engineer"], limit=10, max_per_company=3)
    res.append(show(f"search_jobs ({time.time()-t:.1f}s)", {k: r[k] for k in ("returned", "totalMatched", "companies")} | {"first": r["jobs"][:1]},
                    r["returned"] > 0 and any(c["status"] == "not found" for c in r["companies"])))
    r = await server.search_jobs(["ramp"], remote_only=False, posted_within_days=60, include_description=True, limit=2)
    res.append(show("search_jobs+desc", r["jobs"][:1], r["returned"] > 0 and r["jobs"][0].get("descriptionText")))
    r = await server.list_company_jobs("https://jobs.lever.co/spotify", limit=3)
    res.append(show("list_company_jobs lever", r, r.get("totalJobs", 0) > 0))
    gh = await server.list_company_jobs("airbnb", ats="greenhouse", keyword="data", limit=2)
    res.append(show("list_company_jobs gh", gh, gh.get("totalJobs", 0) > 0))
    j = gh["jobs"][0]
    r = await server.get_job("greenhouse", "airbnb", j["jobId"])
    res.append(show("get_job gh", {**r, "descriptionText": (r.get("descriptionText") or "")[:200]}, bool(r.get("descriptionText"))))
    lv = (await server.list_company_jobs("lever:spotify", limit=1))["jobs"][0]
    r = await server.get_job(job_url=lv["url"])
    res.append(show("get_job lever url", {**r, "descriptionText": (r.get("descriptionText") or "")[:200]}, bool(r.get("descriptionText"))))
    ab = (await server.list_company_jobs("ashby:ramp", limit=1))["jobs"][0]
    r = await server.get_job("ashby", "ramp", ab["jobId"])
    res.append(show("get_job ashby", {**r, "descriptionText": (r.get("descriptionText") or "")[:200]}, bool(r.get("descriptionText"))))
    r = await server.get_job("lever", "spotify", "00000000-0000-0000-0000-000000000000")
    res.append(show("get_job missing", r, "error" in r))
    for q in ["https://jobs.lever.co/spotify", "ramp.com", "Airbnb", "Zzqx Nonexistent Corp"]:
        t = time.time()
        r = await server.detect_ats(q)
        ok = bool(r.get("boards")) if "Zzqx" not in q else r.get("boards") == []
        res.append(show(f"detect_ats {q!r} ({time.time()-t:.1f}s)", r, ok))
    print(f"{sum(map(bool, res))}/{len(res)} passed")


asyncio.run(main())
