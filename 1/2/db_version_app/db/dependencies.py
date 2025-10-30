import asyncpg
from fastapi import Request

from db_version_app.db.pool import get_db_pool


async def get_pg_connection(
        request: Request = None
) -> asyncpg.Connection:
    """
    Returns currently created and initialized db pool connection.
    If FastAPI app is initialized, it uses a pool created during startapp,
    otherwise creates a new one.

    Parameters
    ----------
    request: Request
        Current request object, if exists.

    Returns
    -------
    asyncpg.Connection
        One connection from the pool ready to use.
    """
    if request and request.app.state and hasattr(request.app.state, 'db_pool'):
        db_pool = request.app.state.db_pool
    else:
        db_pool = await get_db_pool()
    async with db_pool.acquire() as connection:
        yield connection
