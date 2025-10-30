import pytest
from fastapi.testclient import TestClient

from db_version_app.web.app import create_app


@pytest.fixture(scope="function")
def test_client():
    return TestClient(app=create_app())

