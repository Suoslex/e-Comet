import asyncpg

from db_version_app.settings import settings


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