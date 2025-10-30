from github_clickhouse_saver.db.repository import ClickhouseRepository
from github_clickhouse_saver.schemas import (
    RepositoryDTO,
    RepositoriesPositionsDTO,
    RepositoriesAuthorsCommitsDTO
)


class RepositoriesRepository(ClickhouseRepository[RepositoryDTO]):
    __tablename__ = "repositories"


class RepositoriesAuthorsCommitsRepository(
    ClickhouseRepository[RepositoriesAuthorsCommitsDTO]
):
    __tablename__ = "repositories_authors_commits"


class RepositoriesPositionsRepository(
    ClickhouseRepository[RepositoriesPositionsDTO]
):
    __tablename__ = "repositories_positions"
