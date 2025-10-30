from typing import Annotated

import asyncpg
from fastapi import Depends, HTTPException

from db_version_app.db import get_pg_connection


async def get_db_version(
        conn: Annotated[asyncpg.Connection, Depends(get_pg_connection)]
) -> str:
    try:
        return await conn.fetchval("SELECT version()")
    except asyncpg.PostgresError:
        raise HTTPException(
            status_code=502,
            detail="Database is not available at the moment."
        )
