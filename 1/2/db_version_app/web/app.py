from contextlib import asynccontextmanager

from fastapi import FastAPI

from db_version_app.settings import settings
from db_version_app.db.pool import get_db_pool
from db_version_app.web.api.views import router as db_version_app_router


def create_app() -> FastAPI:
    app = FastAPI(title="e-Comet", debug=settings.debug, lifespan=lifespan)
    register_routes(app)
    return app


def register_routes(app: FastAPI):
    routes = (
        (db_version_app_router, "/api"),
    )
    for router, path in routes:
        app.include_router(router, prefix=path)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_pool = await get_db_pool()
    yield
    await app.state.db_pool.close()
