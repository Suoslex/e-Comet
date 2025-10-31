import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock

from db_version_app.app import create_app
from db_version_app.db import get_pg_connection


@pytest.fixture(scope="function")
def test_client():
    app = create_app()
    mock_db = AsyncMock()
    mock_db.fetchval.return_value = "1.2.3"
    app.dependency_overrides[get_pg_connection] = lambda: mock_db
    return TestClient(app=app)

