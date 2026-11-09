"""Airflow API clients built from the deployment's settings

``make_client()`` returns the official client's ``ApiClient``, and plugins use
its API classes on it::

    api_client = make_client(settings, token=token, user_agent="graph-toolbox/0.4.0")
    run = DagRunApi(api_client).trigger_dag_run(
        "marc_import",
        TriggerDAGRunPostBody(dag_run_id=run_id, conf=conf),
        _request_timeout=settings.request_timeout,
    )

The client has no default timeout, so pass ``_request_timeout`` on every call.
"""

import urllib3
from airflow_client.client import ApiClient, Configuration

from bluecore_airflow_common.integration.settings import BlueCoreSettings


def make_client(
    settings: BlueCoreSettings, *, token: str, user_agent: str
) -> ApiClient:
    """An Airflow API client for the user with ``token``"""
    retries = urllib3.Retry(
        total=settings.max_retries,
        allowed_methods={"GET", "HEAD"},
        status_forcelist={502, 503, 504},
        backoff_factor=0.5,
        respect_retry_after_header=True,
        redirect=False,
        raise_on_status=False,
    )
    configuration = Configuration(
        host=settings.airflow_internal_url, access_token=token, retries=retries
    )
    api_client = ApiClient(configuration)
    api_client.user_agent = user_agent
    return api_client
