# Blue Core integration contracts

What a Blue Core Airflow plugin, such as the Graph Toolbox or the workflows Bulk
and Reports plugins, can rely on from the Blue Core API, Airflow and the shared 
volumes. For each capability it gives the owner, the method and path, the auth, 
the schemas, the limits, whether it can safely be repeated, and its status:

- **exists**: deployed, and plugins can use it as described
- **partial**: deployed, with the gaps listed
- **missing**: not built; the blocking issue is linked, and no endpoint is
  described for it

Paths are relative to `BLUECORE_API_INTERNAL_URL` for the Blue Core API and
`AIRFLOW_API_INTERNAL_URL` for Airflow. Plugins reach both through the
factories in `bluecore_airflow_common.integration` and never take a base URL
from a request.

## Service ownership

| Concern | Owner | How plugins reach it |
|---|---|---|
| Conversion (MARC ⇄ BIBFRAME) | [`marc-bibframe`](https://pypi.org/project/marc-bibframe/), run in-process by plugins, the Blue Core API and the `marc_to_bibframe` DAG | `integration.convert` |
| Persistence | Blue Core API and its database; the `resource_loader`, `archived_file_loader` and `bulk_update` DAGs also write to it | `integration.blue_core.make_client()` |
| LSP export (FOLIO, Alma) | bluecore-workflows `monitor_institutions_exports` DAG, started by the Blue Core API's `POST /export/` | the Blue Core API |
| Job execution | Airflow, running bluecore-workflows DAGs | `integration.airflow.make_client()` |
| Report storage | the reports volume (`BLUECORE_REPORTS_DIR`), written by bluecore-workflows DAGs | `integration.reports` |
| Report browsing | the workflows Reports plugin ([bluecore-workflows#191](https://github.com/blue-core-lod/bluecore-workflows/issues/191)), and each plugin for its own runs | `integration.reports` |
| Search and graph analysis | the browser, with PyScript and RDFLib | not proxied: there is no backend SPARQL endpoint |

## Errors

Every capability below fails with the shared error body from
`integration.errors.install_error_handler(app)`:

```json
{"error": {"code": "upstream_timeout", "message": "…", "upstream_status": null, "request_id": "…"}}
```

The status and code mapping is in `errors.py`. Upstream bodies are logged 
under the `request_id` and never returned. A capability that doesn't exist 
upstream raises `NotImplementedUpstream`, a 501 `not_available` that links 
its blocking issue.

## Conversion

### Single record: **exists**

| | |
|---|---|
| Owner | `marc-bibframe`, in the plugin's process |
| Call | `convert.marc2xml(marc)`, `convert.marc2bibframe(marc, baseuri=…)`, `convert.bibframe2marc(rdf, …)` |
| Auth | none: no upstream call. `bibframe2marc` looks up authorities on id.loc.gov unless given `authority_lookup=False` |
| Schemas | in: binary MARC21 or MARCXML (a UTF-8 BOM is dropped), or BIBFRAME RDF. Out: MARCXML `<collection>` bytes, or an `rdflib.Graph` |
| Limits | `max_bytes`, default 2 GiB, checked before parsing: 413 `input_too_large`. Unreadable input: 422 `invalid_input` |
| Idempotency | pure function, safe to repeat |

Call it from a plain `def` route so FastAPI runs it in its threadpool.
Conversion is CPU-bound and would block the event loop in an `async def`
route.

### Bulk: **partial**

| | |
|---|---|
| Owner | bluecore-workflows `marc_to_bibframe` DAG |
| Call | `uploads.stage_upload(…)`, then `DagRunApi.trigger_dag_run("marc_to_bibframe", TriggerDAGRunPostBody(conf={"marc_file": path, "ingest": False, "source_base_uri": …}))`: Airflow `POST /api/v2/dags/marc_to_bibframe/dagRuns` |
| Auth | `auth.AirflowToken` |
| Schemas | `conf` as above; `marc_file` is a `.mrc`, `.marc`, `.dat` or `.xml` path on the uploads volume. Response: `DAGRunResponse` |
| Limits | uploads are capped at 2 GiB by `stage_upload`; the DAG holds the whole file in memory |
| Idempotency | not retried. See [Bulk submission](#bulk-submission-and-cancellation) |


## Bounded search and resource loading

### Search: **exists**

| | |
|---|---|
| Owner | Blue Core API |
| Call | `client.search(…)` on `blue_core.make_client(settings, user_agent=…)`: `GET /search/?q=&type=&scope=&limit=&offset=` |
| Auth | none: anonymous |
| Schemas | `SearchResultSchema`: results plus `links` with `first`, `prev` and `next` |
| Limits | `limit` defaults to 20, at most 100. Each request is bounded by `BLUECORE_REQUEST_TIMEOUT` |
| Idempotency | GET, retried on connect errors, read timeouts, 502, 503 and 504, up to `BLUECORE_MAX_RETRIES` |

`scope=identifier` makes `q` an exact identifier lookup, for example
`isbn:9780140449112`.

### Resource loading: **exists**

| | |
|---|---|
| Owner | Blue Core API |
| Call | `client.works.get(uuid)`, `client.instances.get(uuid)`, `client.hubs.get(uuid)`: `GET /works/{uuid}`, `/instances/{uuid}`, `/hubs/{uuid}`; `GET /resources/?limit=&offset=` |
| Auth | none: anonymous |
| Schemas | bluecore-client's plain dictionaries, unvalidated. A body that isn't JSON is a 500 |
| Limits | one resource per request; `/resources/` pages default to 10 |
| Idempotency | GET, retried as for search |

A Hub UUID passed to `works.get` is a 502, not a 404. Graph analysis and SPARQL
over loaded resources run in the browser. There is deliberately no backend
SPARQL proxy.

## Bulk submission and cancellation

### Submission: **exists**

| | |
|---|---|
| Owner | Airflow; each plugin chooses its own DAG ids |
| Call | `DagRunApi(api_client).trigger_dag_run(dag_id, TriggerDAGRunPostBody(dag_run_id=…, conf=…), _request_timeout=settings.request_timeout)`: `POST /api/v2/dags/{dag_id}/dagRuns` |
| Auth | `auth.AirflowToken`, minted for the signed-in user; Airflow checks whether that user may run the DAG |
| Schemas | `TriggerDAGRunPostBody` in, `DAGRunResponse` out. Put the caller's Keycloak subject in `conf["user_uid"]` so reports can be matched to them |
| Limits | input files go through `uploads.stage_upload` (2 GiB, allowed suffixes) and are passed by path, never inline in `conf` |
| Idempotency | **never retried automatically.** To resubmit safely, choose the `dag_run_id`, then catch `ConflictException` (409) and fetch the existing run |

The Blue Core API also starts DAGs, `resource_loader` and
`archived_file_loader` from `POST /batches/` and `POST /batches/upload/`, but it
uses its own Airflow account, so its runs aren't attributed to the user in
Airflow (only in `conf["user_uid"]`).


### Cancellation: **partial**

| | |
|---|---|
| Owner | Airflow |
| Call | `DagRunApi.patch_dag_run(dag_id, dag_run_id, DAGRunPatchBody(state="failed"))`: `PATCH /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}` |
| Auth | `auth.AirflowToken` |
| Schemas | `DAGRunPatchBody` in, `DAGRunResponse` out |
| Idempotency | not retried. Patching a finished run overwrites its state, so read the run first and only patch a `queued` or `running` one |

Airflow has no cancelled state: a cancelled run is `failed`, the same as a run
that failed on its own.

## Job status: **exists**

| | |
|---|---|
| Owner | Airflow |
| Call | `DagRunApi.get_dag_run(dag_id, dag_run_id)`: `GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}`; `TaskInstanceApi.get_task_instances(dag_id, dag_run_id)`: `GET …/taskInstances` |
| Auth | `auth.AirflowToken` |
| Schemas | `DAGRunResponse`, with `state` a `DagRunState`: `queued`, `running`, `success` or `failed`. An unknown state raises pydantic's `ValidationError`, a 500, so `apache-airflow-client` stays pinned to the Airflow minors CI tests |
| Limits | each call takes `_request_timeout=settings.request_timeout`; the generated client has no default |
| Idempotency | GET, retried on 502, 503 and 504 |


## Report retrieval: **partial**

| | |
|---|---|
| Owner | bluecore-workflows writes them; plugins read them |
| Call | `reports.run_dir(dag_id, run_id)`, then fixed names in it: `report.json`, `index.html` |
| Auth | Airflow login on the plugin route, then `reports.owner_matches(dag_run.conf, user_uid, roles=…, admin_roles=…)`. A run without `user_uid` is visible to admins only |
| Schemas | `report.json` is the report as each DAG assembles it; there's no shared schema yet |
| Limits | ids that slug to `""`, `.` or `..` are a 404; file names never come from the request |
| Idempotency | read-only |

Only `bulk_update` writes a report today. Still missing:

- Blue Core API reporting routes:
  [bluecore_api#61](https://github.com/blue-core-lod/bluecore_api/issues/61)
  and [bluecore_api#12](https://github.com/blue-core-lod/bluecore_api/issues/12)
- reports for `resource_loader` and `archived_file_loader`, and the Reports
  plugin that lists them:
  [bluecore-workflows#191](https://github.com/blue-core-lod/bluecore-workflows/issues/191)
- the duplicate report for MARC imports:
  [bluecore-workflows#115](https://github.com/blue-core-lod/bluecore-workflows/issues/115)
- a report from `marc_to_bibframe` (see [Bulk](#bulk-partial))

## PyScript: Subresource Integrity and CSP

Plugin pages run on the Airflow API server's origin with the user's Airflow JWT
cookie, so any script they load runs with that user's session. Pages that use
PyScript include the shared, pinned tags:

```html
{% block head %}{% include "bluecore/_pyscript.html" %}{% endblock %}
```

They load `core.css` and `core.js` from `https://pyscript.net/releases/<version>/`
with `integrity="sha384-…"` and `crossorigin="anonymous"`. To move every
plugin to a new release, run `uv run python scripts/pyscript_sri.py <version>`
and release this package.

SRI only covers those two files. `core.js` then imports its own chunks from the
same release directory, and PyScript loads Pyodide from `cdn.jsdelivr.net`
and packages such as RDFLib from PyPI. None of those carry a hash, so the page's
`Content-Security-Policy` has to limit where they can come from. A starting
point for 2026.7.3:

```
script-src 'self' https://pyscript.net/releases/2026.7.3/ https://cdn.jsdelivr.net/pyodide/ 'wasm-unsafe-eval';
style-src 'self' https://pyscript.net/releases/2026.7.3/;
connect-src 'self' https://cdn.jsdelivr.net/pyodide/ https://pypi.org https://files.pythonhosted.org;
worker-src 'self' blob:;
object-src 'none';
base-uri 'none'
```

## Deployment notes for bluecore-stack

- Set `AIRFLOW_API_INTERNAL_URL` (for example
  `http://airflow-apiserver:8080/workflows`) for every container that loads
  these plugins, along with `BLUECORE_API_INTERNAL_URL` and `BLUECORE_URL`.
  Missing values are logged at import, and requests fail until they're set.
- Raise nginx's `client_max_body_size` on the Airflow location to the plugins'
  upload limit (2 GiB by default), and size container memory for conversions
  near that limit: `marc-bibframe` and `marc_to_bibframe` hold the whole input
  and its graph in memory.
- Mount the reports volume at `/opt/airflow/reports` (or set
  `BLUECORE_REPORTS_DIR`) in every Airflow container. `compose-base.yaml`
  mounts uploads but not reports. Uploads are read from `BLUECORE_UPLOADS_DIR`,
  default `/opt/airflow/uploads`.