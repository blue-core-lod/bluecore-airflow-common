"""Shared clients for the Blue Core API and the Airflow REST API.

Nothing is re-exported here: import what you need from its own module, such as
``from bluecore_airflow_common.integration.settings import get_settings``.

This subpackage is never imported by ``bluecore_airflow_common`` itself, so
plugins that only use the shared templates and static files don't load HTTP
clients or need the ``[bluecore]`` configuration.
"""
