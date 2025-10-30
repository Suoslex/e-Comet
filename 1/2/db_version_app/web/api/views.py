from typing import Annotated

from fastapi import APIRouter, Depends

from db_version_app.web.api.services import DBVersionAppService


router = APIRouter()


@router.get("/db_version")
async def get_db_version(
        db_version_app_service: Annotated[
            DBVersionAppService,
            Depends(DBVersionAppService)
        ]
) -> str:
    """Returns current DB version as a string."""
    return await db_version_app_service.get_db_version()


