# bluecore-airflow-common

Shared helpers, base template and styles for Blue Core Airflow 3 plugins.

## Install

```sh
uv add bluecore-airflow-common
```

## Usage

```python
from fastapi import Depends, FastAPI, Request

import bluecore_airflow_common as common

URL_PREFIX = "/my-plugin"

app = FastAPI()
common.mount_static(app, "static")
templates = common.templates("templates")


@app.get("/", dependencies=[Depends(common.requires_login)])
def index(request: Request):
    return templates.TemplateResponse(
        request, "index.html", common.context(URL_PREFIX, title="My Plugin")
    )
```

Plugin pages extend the shared base template:

```html
{% extends "bluecore/base.html" %}
{% block content %}<h1>{{ title }}</h1>{% endblock %}
```

Shared assets are served at `{url_prefix}/static/bluecore/`, and a plugin's
own files at `{url_prefix}/static/`. In tests, switch off authentication with
`app.dependency_overrides[common.requires_login] = lambda: None`.

### PyScript

Pages that run PyScript include the shared tags, which pin one release with
Subresource Integrity:

```html
{% block head %}{% include "bluecore/_pyscript.html" %}{% endblock %}
```

To move to a new release, run `uv run python scripts/pyscript_sri.py <version>`.
The Content-Security-Policy these pages need is in
[docs/integration-contracts.md](docs/integration-contracts.md#pyscript-subresource-integrity-and-csp).

## Integration

`bluecore_airflow_common.integration` builds clients for the Blue Core API
([`bluecore-client`](https://pypi.org/project/bluecore-client/)) and Airflow's
`/api/v2` ([`apache-airflow-client`](https://pypi.org/project/apache-airflow-client/)),
and reads reports and stages uploads on the shared volumes. `import bluecore_airflow_common`
doesn't load it, so plugins that only use the templates don't need its
configuration. What each upstream capability offers, and what's still missing,
is in [docs/integration-contracts.md](docs/integration-contracts.md).

### Configuration

Read from the environment, which bluecore-stack sets from `.env`:

| Variable | |
|---|---|
| `BLUECORE_API_INTERNAL_URL` | required. The Blue Core API as the Airflow containers reach it, e.g. `http://bc_api:8100` |
| `BLUECORE_URL` | required. The Blue Core site as the browser reaches it, e.g. `https://bcld.info` |
| `AIRFLOW_API_INTERNAL_URL` | required. Airflow as the plugins reach it, e.g. `http://airflow-apiserver:8080/workflows` |
| `BLUECORE_REQUEST_TIMEOUT` | seconds per upstream request, default 10 |
| `BLUECORE_MAX_RETRIES` | retries for GET and HEAD, default 2, at most 5 |
| `BLUECORE_REPORTS_DIR` | the reports volume, default `/opt/airflow/reports` |
| `BLUECORE_UPLOADS_DIR` | the uploads volume, default `/opt/airflow/uploads` |

Missing settings are logged when the module is imported, and raise
`SettingsError` when first used. `public_config()` returns the only settings
that are safe to send to a browser.

### Usage

```python
from typing import Annotated

from airflow_client.client import DagRunApi
from fastapi import Depends, FastAPI

from bluecore_airflow_common.integration import airflow, auth, blue_core
from bluecore_airflow_common.integration.errors import install_error_handler
from bluecore_airflow_common.integration.settings import BlueCoreSettings, get_settings

USER_AGENT = "my-plugin/1.0.0"

app = FastAPI()
install_error_handler(app)
auth.install_token_redaction()

Settings = Annotated[BlueCoreSettings, Depends(get_settings)]


@app.get("/works/{uuid}")
def work(uuid: str, settings: Settings) -> dict:
    with blue_core.make_client(settings, user_agent=USER_AGENT) as client:
        return client.works.get(uuid)


@app.get("/jobs/{run_id}")
def job(run_id: str, settings: Settings, token: auth.AirflowToken) -> dict:
    api_client = airflow.make_client(settings, token=token, user_agent=USER_AGENT)
    run = DagRunApi(api_client).get_dag_run(
        "marc_to_bibframe", run_id, _request_timeout=settings.request_timeout
    )
    return {"state": run.state}
```

Use plain `def` routes: both clients are synchronous, so
FastAPI runs them in its threadpool. Reads from Blue Core are anonymous; for a
write, pass `token=` from `auth.KeycloakToken` to `blue_core.make_client`.
Upstream failures come back as
`{"error": {"code", "message", "upstream_status", "request_id"}}`.

### Testing

Override the settings and tokens with FastAPI's dependency overrides, and mock
the HTTP layer underneath the clients:

```python
from bluecore_airflow_common.integration import auth
from bluecore_airflow_common.integration.settings import BlueCoreSettings, get_settings

app.dependency_overrides[get_settings] = lambda: BlueCoreSettings(
    internal_url="http://bluecore.test",
    public_url="https://bluecore.test",
    airflow_internal_url="http://airflow.test",
)
app.dependency_overrides[auth.airflow_token] = lambda: "test-token"
app.dependency_overrides[auth.keycloak_token] = lambda: "test-token"
```

- Blue Core: mock `http://bluecore.test` with
  [`respx`](https://lundberg.github.io/respx/), since bluecore-client uses httpx.
- Airflow: patch the API class your route calls, such as
  `DagRunApi.get_dag_run`, or point `airflow_internal_url` at a local server.
- Tests that read the environment instead should call `get_settings.cache_clear()`
  after changing it.
- Point `BLUECORE_REPORTS_DIR` and `BLUECORE_UPLOADS_DIR` at `tmp_path`, or
  pass `base_dir=` to `reports.run_dir` and `uploads.stage_upload`.

  
## Development

```sh
uv run ruff format && uv run ruff check
uv run ty check
uv run pytest
```
