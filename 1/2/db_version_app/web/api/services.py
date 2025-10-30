from typing import Annotated

import asyncpg
from fastapi import Depends, HTTPException

from db_version_app.db.dependencies import get_pg_connection


class DBVersionAppService:
    def __init__(
            self,
            db_connection: Annotated[
                asyncpg.Connection,
                Depends(get_pg_connection)
            ]
    ):
        self._db_connection = db_connection

    async def get_db_version(self) -> str:
        """Returns current DB version as a string"""
        try:
            return await self._db_connection.fetchval("SELECT version()")
        except asyncpg.PostgresError:
            raise HTTPException(
                status_code=502,
                detail="Database is not available at the moment."
            )
