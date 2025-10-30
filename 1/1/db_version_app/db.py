import asyncpg
from fastapi import Request

from db_version_app.settings import settings


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


def get_db_pool(
        user=settings.db_user,
        password=settings.db_password,
        database=settings.db_database,
        host=settings.db_host,
        port=settings.db_port,
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size
) -> asyncpg.Pool:
    """
    Returns an object of DB pool to work with.
    Make sure you await it before using, so that it's initialized.

    Parameters
    ----------
    user: str
        DB username to login
    password: str
        DB password of the username
    database: str
        Database to use
    host: str
        Hostname of the database
    port: int
        Port to use to connect to the DB.
    min_size: int
        Minimum size of the connection pool
    max_size: int
        Maximum size of the connection pool

    Returns
    -------
    asyncpg.Pool
        Database connection pool
    """
    return asyncpg.create_pool(
        user=user,
        password=password,
        database=database,
        host=host,
        port=port,
        min_size=min_size,
        max_size=max_size,
    )
