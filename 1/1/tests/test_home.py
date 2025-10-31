import asyncpg
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock

from db_version_app.db import get_pg_connection


def test_main_api_returns_200(test_client: TestClient):
    response = test_client.get("/api/db_version")
    assert response.status_code == 200


def test_main_api_returns_db_version(test_client: TestClient):
    response = test_client.get("/api/db_version")
    assert response.status_code == 200
    assert response.json() == "1.2.3"


@pytest.mark.parametrize(
    "error",
    [
        asyncpg.ConnectionRejectionError,
        asyncpg.ConnectionFailureError,
        asyncpg.PostgresConnectionError,
        asyncpg.PostgresError
    ]
)
def test_api_returns_not_available_on_db_errors(
        test_client: TestClient,
        error
):
    mock_db = AsyncMock()
    mock_db.fetchval.side_effect = error()
    test_client.app.dependency_overrides[get_pg_connection] = lambda: mock_db
    response = test_client.get("/api/db_version")
    assert response.status_code == 502
    assert "not available" in response.json()['detail']

