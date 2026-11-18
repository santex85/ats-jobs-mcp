# ATS Jobs MCP server (Greenhouse, Lever, Ashby)

An MCP server that gives AI agents live access to open job postings from any company that hires through
**Greenhouse**, **Lever** or **Ashby**. It reads the platforms' official, public, no-auth job-board APIs (the
JSON feeds behind each company's careers page) and returns every job in one unified, flat schema: title,
department, team, location, remote/hybrid, employment type, salary range when the employer publishes one,
posting date, apply link, and optionally the full description as plain text.

It is a port of the `ats-jobs-api` Apify actor. The normalisation is the same; the Apify and billing code is removed.

## Tools

| Tool | What it does |
|---|---|
| `search_jobs` | Search open jobs across up to 20 companies with keyword, location, department, remote-only and "posted within N days" filters |
| `list_company_jobs` | All open jobs of one company in compact form, newest first, plus job counts per department |
| `get_job` | One posting with its full plain-text description, by `ats + company + job_id` or by job URL |
| `detect_ats` | Find which ATS a company uses and its board slug, from a name, a domain, a careers URL or an ATS URL |

### Example calls

```jsonc
// search_jobs
{"companies": ["greenhouse:airbnb", "lever:spotify", "https://jobs.ashbyhq.com/ramp", "linear"],
 "keywords": ["engineer", "data"], "locations": ["Remote", "London"], "posted_within_days": 30,
 "limit": 20, "max_per_company": 5}

// list_company_jobs
{"company": "spotify", "ats": "lever", "keyword": "data", "limit": 50}

// get_job
{"ats": "greenhouse", "company": "airbnb", "job_id": "8396777"}
{"job_url": "https://jobs.lever.co/spotify/1a1c0754-29b1-4651-a399-3c078866b9d0"}

// detect_ats
{"query": "https://www.notion.com/careers"}   // -> ashby / notion (link found on the page)
{"query": "Airbnb"}                           // -> greenhouse / airbnb (slug guess)
```

Company inputs can be a bare slug (`airbnb`, ATS auto-detected), `ats:slug` (`greenhouse:airbnb`,
`lever:spotify`, `ashby:ramp`, `lever-eu:<slug>`) or a board URL. In auto mode the server queries Greenhouse, Lever
and Ashby in parallel and uses the board with the most open jobs, because the same slug sometimes exists on
several platforms as a stale board. For example, `ramp` has a one-job test board on Greenhouse and 180+ jobs on Ashby.

### Job record fields

`ats, company, companySlug, jobId, title, department, team, location, allLocations, country, workplaceType
(remote/hybrid/onsite/null), isRemote, employmentType, compensationText, salaryMin, salaryMax, salaryCurrency,
salaryInterval, postedAt, updatedAt, applyUrl, url, requisitionId, language`, plus `descriptionText` when requested.
A field is `null` when the platform or employer does not provide it.

## Run

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python src/server.py                         # streamable-http on 0.0.0.0:8080, endpoint /mcp
PORT=8091 .venv/bin/python src/server.py               # other port
MCP_TRANSPORT=stdio .venv/bin/python src/server.py     # stdio, for local MCP clients
.venv/bin/python tests/live_test.py                    # live smoke test of every tool
```

Environment: `HOST` (default `0.0.0.0`), `PORT` (default `8080`), `MCP_TRANSPORT` (`streamable-http` | `stdio` | `sse`).

## Limits

- `search_jobs`: max 20 companies and max 200 jobs per call. Descriptions are capped at 3,000 characters.
- `list_company_jobs`: max 200 jobs returned. `totalJobs` and the department counts always cover the whole board.
- `get_job`: description capped at 20,000 characters.
- Every tool call has a 120 s timeout. Each HTTP request has a 30 s timeout and up to 3 attempts with backoff on 429/5xx.
- Polite per-host rate limiting: at most 1 request per 1 s to Lever (its robots.txt asks for `Crawl-delay: 1`) and
  1 per 0.5 s to Greenhouse and Ashby. Lever pagination stops at about 2,100 postings per board.
- `detect_ats` fetches at most 2 public pages (max 2 MB each) and probes up to 2 slug guesses on 3 platforms.
  A guessed slug can belong to a different company with the same name, so check `boardName` or a job URL.
  It cannot see boards that a site loads only through JavaScript, and it does not cover Workday, SmartRecruiters or other ATS.
- Ashby has no single-posting endpoint, so `get_job` for Ashby reads the whole board and picks the job.
- Greenhouse does not expose employment type or remote status as fields. The server uses the employer's custom
  metadata when present and otherwise infers remote or hybrid from the location text.

## Data sources and terms

- Greenhouse Job Board API: `boards-api.greenhouse.io/v1/boards/{slug}/jobs` ([docs](https://developers.greenhouse.io/job-board.html))
- Lever Postings API: `api.lever.co/v0/postings/{slug}` and `api.eu.lever.co` ([docs](https://github.com/lever/postings-api))
- Ashby Job Postings API: `api.ashbyhq.com/posting-api/job-board/{slug}` ([docs](https://developers.ashbyhq.com/docs/public-job-posting-api))

These are public endpoints that the platforms publish so that job listings can be displayed. Job content belongs to
the employers: link back to the original posting (`url` / `applyUrl`) when you republish jobs, and follow each
employer's and platform's terms. Lever's career-site robots.txt signals `ai-train=no`, so don't use the output to train
AI models. This project is not affiliated with Greenhouse, Lever (Employ Inc.) or Ashby.

## Privacy

The server returns job posting data only. It never outputs recruiter or hiring-manager names, e-mail addresses or
phone numbers, and replaces e-mail addresses inside descriptions with `[email removed]`. Ashby postings marked as
unlisted ("direct link only") are skipped. The server keeps no logs of queries or results.

## Use with Claude Desktop or another MCP client

Local (stdio). Clone the repo, create the venv as above, then add this to `claude_desktop_config.json`
(or your client's MCP config), with the paths changed to where you cloned it:

```json
{
  "mcpServers": {
    "ats-jobs": {
      "command": "/path/to/ats-jobs-mcp/.venv/bin/python",
      "args": ["/path/to/ats-jobs-mcp/src/server.py"],
      "env": {"MCP_TRANSPORT": "stdio"}
    }
  }
}
```

Remote (streamable HTTP). Run `.venv/bin/python src/server.py` on a server and point any client that supports
remote MCP servers at `http://<host>:8080/mcp`. `GET /health` returns `{"status": "healthy"}`.

## License

MIT, see [LICENSE](LICENSE). The data returned by the tools stays under the terms of its sources (see above).
