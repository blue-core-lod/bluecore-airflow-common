from datetime import UTC, datetime
from uuid import UUID

import httpx
import pydantic
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bluecore_airflow_common.integration import errors, schemas

WORK_UUID = "5f1b9d0c-3c43-4b5e-9d2e-1a2b3c4d5e6f"

# A search result whose uri, uuid and timestamps were null, so left out
HUB_RESULT = {"id": 2, "type": "hubs", "data": {}}

# Shaped like Blue Core's GET /api/search/, which leaves out null fields
SEARCH_BODY = {
    "results": [
        {
            "id": 1,
            "type": "works",
            "data": {"@id": "https://bcld.info/works/1", "@type": "Work"},
            "uri": "https://bcld.info/works/1",
            "uuid": WORK_UUID,
            "created_at": "2026-10-01T12:00:00Z",
            "updated_at": "2026-10-02T12:00:00Z",
        },
        HUB_RESULT,
    ],
    "links": {"first": "https://bcld.info/api/search/?limit=2&offset=0"},
    "total": 2,
}

# Shaped like GET /api/instances/{uuid} with Accept: application/vnd.sinopia+json
INSTANCE_BODY = {
    "id": 7,
    "type": "instances",
    "data": {"@id": "https://bcld.info/instances/7"},
    "uri": "https://bcld.info/instances/7",
    "uuid": WORK_UUID,
    "created_at": None,
    "updated_at": None,
    "work_id": 1,
    "is_expanded": False,
}

# Shaped like Airflow 3.3 GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}
DAG_RUN_BODY = {
    "dag_run_id": "manual__marc_to_bibframe__abc123",
    "dag_id": "marc_to_bibframe",
    "logical_date": None,
    "queued_at": "2026-11-03T10:00:00Z",
    "start_date": "2026-11-03T10:00:05Z",
    "end_date": "2026-11-03T10:01:00Z",
    "duration": 55.0,
    "run_after": "2026-11-03T10:00:00Z",
    "run_type": "manual",
    "state": "failed",
    "triggered_by": "rest_api",
    "conf": {"user_uid": "jpnelson"},
    "note": None,
    "dag_versions": [],
    "dag_display_name": "marc_to_bibframe",
}

# Shaped like Airflow 3.3 GET .../dagRuns/{dag_run_id}/taskInstances
TASK_INSTANCES_BODY = {
    "task_instances": [
        {
            "id": "0198c3c4-0000-7000-8000-000000000001",
            "task_id": "convert",
            "dag_id": "marc_to_bibframe",
            "dag_run_id": "manual__marc_to_bibframe__abc123",
            "map_index": -1,
            "state": "failed",
            "try_number": 2,
            "max_tries": 1,
            "start_date": "2026-11-03T10:00:05Z",
            "end_date": "2026-11-03T10:01:00Z",
            "pool": "default_pool",
        },
        {
            "task_id": "ingest",
            "dag_id": "marc_to_bibframe",
            "dag_run_id": "manual__marc_to_bibframe__abc123",
            "state": None,
            "try_number": 0,
        },
    ],
    "total_entries": 2,
}


def upstream_response(
    body: object = None, *, content: bytes | None = None
) -> httpx.Response:
    """A 200 from the Blue Core API with a JSON body, or raw content if given"""
    request = httpx.Request("GET", "http://bc_api:8100/search/")
    if content is not None:
        return httpx.Response(200, request=request, content=content)
    return httpx.Response(200, request=request, json=body)


def with_extra(body: dict, **extra) -> dict:
    return {**body, **extra}


def test_every_model_ignores_unknown_fields_and_is_frozen():
    """Each upstream model ignores undeclared fields and can't be modified,
    as the issue requires extra="ignore" for every response model."""
    models = [
        schemas.Resource,
        schemas.Links,
        schemas.SearchResults,
        schemas.DagRun,
        schemas.TaskInstance,
        schemas.TaskInstances,
    ]

    for model in models:
        assert issubclass(model, schemas.UpstreamModel)
        assert model.model_config.get("extra") == "ignore"
        assert model.model_config.get("frozen") is True


def test_search_results_parse():
    """A Blue Core search response parses, including a result whose null
    fields were left out by the API's response_model_exclude_none."""
    results = schemas.parse(schemas.SearchResults, upstream_response(SEARCH_BODY))

    work, hub = results.results
    assert results.total == 2
    assert work.type == "works"
    assert work.uuid == UUID(WORK_UUID)
    assert work.created_at == datetime(2026, 10, 1, 12, tzinfo=UTC)
    assert hub.type == "hubs"
    assert hub.uri is None
    assert hub.uuid is None
    assert results.links.next is None


def test_search_results_ignore_new_fields():
    """New fields from a future Blue Core release, at the top level or on a
    result, are dropped instead of failing the response."""
    body = with_extra(SEARCH_BODY, facets={"type": {}})
    body["results"] = [with_extra(HUB_RESULT, score=0.9)]

    results = schemas.parse(schemas.SearchResults, upstream_response(body))

    assert results.results[0].id == 2
    assert not hasattr(results, "facets")


def test_full_resource_parses_with_explicit_nulls():
    """A vnd.sinopia+json resource, which sends null fields explicitly and
    includes work_id, parses; is_expanded is ignored."""
    instance = schemas.parse(schemas.Resource, upstream_response(INSTANCE_BODY))

    assert instance.type == "instances"
    assert instance.work_id == 1
    assert instance.hub_id is None
    assert instance.created_at is None


def test_bare_json_ld_is_not_a_resource():
    """Asking for application/json returns bare JSON-LD with no type or data
    wrapper; parsing that as a Resource is a 502, not a half-empty model."""
    bare = {"@context": "https://bcld.info/api/context.jsonld", "@id": "x"}

    with pytest.raises(errors.BadUpstreamResponse, match="type, data"):
        schemas.parse(schemas.Resource, upstream_response(bare))


def test_parsed_models_are_frozen():
    """A parsed response can't be changed by accident after it's validated."""
    resource = schemas.parse(schemas.Resource, upstream_response(INSTANCE_BODY))

    with pytest.raises(pydantic.ValidationError):
        resource.type = "works"  # ty: ignore[invalid-assignment]


def test_dag_run_parses_airflow_response():
    """A full Airflow 3.3 dagRun parses; the fields the models leave out,
    such as dag_versions, are ignored."""
    run = schemas.parse(schemas.DagRun, upstream_response(DAG_RUN_BODY))

    assert run.dag_run_id == "manual__marc_to_bibframe__abc123"
    assert run.state == "failed"
    assert run.conf == {"user_uid": "jpnelson"}
    assert run.end_date == datetime(2026, 11, 3, 10, 1, tzinfo=UTC)


def test_dag_run_keeps_unknown_states():
    """state is Airflow's string, so a state added in a later Airflow
    release still parses and is left to AirflowClient to normalize."""
    body = with_extra(DAG_RUN_BODY, state="paused_for_review")

    run = schemas.parse(schemas.DagRun, upstream_response(body))

    assert run.state == "paused_for_review"


def test_task_instances_parse():
    """Airflow task instances parse, including one not yet scheduled, whose
    state is null and which has no map_index or dates."""
    page = schemas.parse(schemas.TaskInstances, upstream_response(TASK_INSTANCES_BODY))

    convert, ingest = page.task_instances
    assert page.total_entries == 2
    assert convert.state == "failed"
    assert convert.try_number == 2
    assert ingest.state is None
    assert ingest.map_index == -1
    assert ingest.start_date is None


@pytest.mark.parametrize(
    "content,locations",
    [
        (b"<html>Bad gateway</html>", "body"),
        (b"[]", "body"),
        (b'{"results": "x", "links": {}, "total": "many"}', "results, links.first"),
        (b"{}", "results, links, total"),
    ],
)
def test_parse_rejects_malformed_bodies(content, locations):
    """A body that isn't JSON or doesn't match the model becomes
    BadUpstreamResponse, and the message names the failing fields."""
    with pytest.raises(errors.BadUpstreamResponse) as caught:
        schemas.parse(schemas.SearchResults, upstream_response(content=content))

    assert f"invalid SearchResults: {locations}" in str(caught.value)


def test_parse_error_keeps_body_for_logging_only():
    """The upstream body is kept on the error for the server log, while the
    message lists field locations without echoing any of the values."""
    content = b'{"results": [{"type": "works", "data": "token=abc123"}]}'

    with pytest.raises(errors.BadUpstreamResponse) as caught:
        schemas.parse(schemas.SearchResults, upstream_response(content=content))

    assert caught.value.upstream_status == 200
    assert caught.value.upstream_body == content.decode()
    assert "abc123" not in str(caught.value)


def test_parse_error_becomes_bad_upstream_response():
    """Through the shared error handler, a malformed upstream body is a 502
    bad_upstream_response that doesn't leak the body or internal URL."""
    app = FastAPI()
    errors.install_error_handler(app)

    @app.get("/search")
    def search() -> dict[str, int]:
        content = b'{"results": [], "secret": "abc123"}'
        schemas.parse(schemas.SearchResults, upstream_response(content=content))
        return {}

    response = TestClient(app).get("/search")

    assert response.status_code == 502
    assert response.json()["error"]["code"] == "bad_upstream_response"
    assert "abc123" not in response.text
    assert "bc_api" not in response.text
