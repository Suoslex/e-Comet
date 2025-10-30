from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI

from db_version_app.db import get_db_pool
from db_version_app.settings import settings
from db_version_app.views import get_db_version


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_pool = await get_db_pool()
    yield
    await app.state.db_pool.close()


def register_routes(app: FastAPI):
    router = APIRouter(prefix="/api")
    router.add_api_route(path="/db_version", endpoint=get_db_version)
    app.include_router(router)


def create_app() -> FastAPI:
    app = FastAPI(title="e-Comet", debug=settings.debug, lifespan=lifespan)
    register_routes(app)
    return app
