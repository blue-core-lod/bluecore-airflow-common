"""Models for the upstream responses the integration clients read

Only the fields a client or plugin reads are declared; any other field is
ignored, so a new upstream field doesn't break a plugin. ``parse()`` turns a
body that doesn't match into ``BadUpstreamResponse`` (502).

Blue Core resources follow ``bluecore_api/schemas/schemas.py``. A resource GET
returns this shape only for ``Accept: application/vnd.sinopia+json``; other
JSON types return the bare JSON-LD. Search leaves out null fields, so every
nullable field has a default.

Airflow models follow the ``/api/v2`` ``DAGRunResponse`` and
``TaskInstanceResponse``. ``state`` is kept as Airflow's string;
``AirflowClient`` maps it to the normalized states.
"""

from datetime import datetime
from typing import Any
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError

from bluecore_airflow_common.integration.errors import (
    BadUpstreamResponse,
    _body_excerpt,
)


class UpstreamModel(BaseModel):
    """Base for upstream responses: unknown fields are ignored, values are frozen"""

    model_config = ConfigDict(extra="ignore", frozen=True)


class Resource(UpstreamModel):
    """A Blue Core work, instance or hub; ``type`` says which"""

    type: str
    data: dict[str, Any]
    id: int | None = None
    uri: str | None = None
    uuid: UUID | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    hub_id: int | None = None
    work_id: int | None = None


class Links(UpstreamModel):
    first: str
    prev: str | None = None
    next: str | None = None


class SearchResults(UpstreamModel):
    results: list[Resource]
    links: Links
    total: int


class DagRun(UpstreamModel):
    dag_run_id: str
    dag_id: str
    state: str
    conf: dict[str, Any] | None = None
    queued_at: datetime | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None


class TaskInstance(UpstreamModel):
    task_id: str
    state: str | None = None
    map_index: int = -1
    try_number: int = 0
    start_date: datetime | None = None
    end_date: datetime | None = None


class TaskInstances(UpstreamModel):
    task_instances: list[TaskInstance]
    total_entries: int | None = None


def parse[M: BaseModel](model: type[M], response: httpx.Response) -> M:
    """Validate a JSON response body as ``model``

    Raises:
        BadUpstreamResponse: if the body isn't JSON or doesn't match.
    """
    try:
        return model.model_validate_json(response.content)
    except ValidationError as exc:
        locations = ", ".join(
            ".".join(map(str, error["loc"])) or "body"
            for error in exc.errors(include_input=False)
        )
        raise BadUpstreamResponse(
            f"{response.request.url} returned an invalid {model.__name__}: {locations}",
            upstream_status=response.status_code,
            upstream_body=_body_excerpt(response),
        ) from exc
