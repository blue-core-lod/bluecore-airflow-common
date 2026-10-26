# bluecore-airflow-common

Shared helpers, base template and styles for Blue Core Airflow 3 plugins.

## Install

```sh
uv add "bluecore-airflow-common @ git+https://github.com/blue-core-lod/bluecore-airflow-common"
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

## Development

```sh
uv run ruff format && uv run ruff check
uv run mypy src tests
uv run pytest
```
