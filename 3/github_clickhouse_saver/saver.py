import asyncio
from dataclasses import asdict
from datetime import datetime, UTC

from aiochclient import ChClient

from github_repos_scrapper.schemas import Repository
from github_repos_scrapper.scrapper import GithubReposScrapper

from github_clickhouse_saver.logger import logger
from github_clickhouse_saver.db.init import init_clickhouse
from github_clickhouse_saver.schemas import (
    RepositoryDTO,
    RepositoriesPositionsDTO,
    RepositoriesAuthorsCommitsDTO
)
from github_clickhouse_saver.repositories import (
    RepositoriesRepository,
    RepositoriesPositionsRepository,
    RepositoriesAuthorsCommitsRepository
)

class GithubClickhouseSaver:
    """Saves top Github repositories to clickhouse"""

    def __init__(self, ch_client: ChClient):
        """
        Initialize the GithubClickhouseSaver.

        Parameters
        ----------
        ch_client: ChClient
            aiochclient client object to be used in requests to clickhouse.
        """
        self._initialized = False
        self._ch_client = ch_client
        self._repositories_repository = RepositoriesRepository(ch_client)
        self._repositories_positions_repository = (
            RepositoriesPositionsRepository(ch_client)
        )
        self._repositories_authors_commits_repository = (
            RepositoriesAuthorsCommitsRepository(ch_client)
        )

    async def __aenter__(self):
        """
        Can be used to automatically initialize clickhouse database
        and close connection when the work is done.
        """
        await self.init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._ch_client.close()

    async def init(self):
        """Initializes clickhouse database. Should be used once at startapp."""
        await init_clickhouse(self._ch_client)
        self._initialized = True

    async def save_top_repos(
            self,
            access_token: str,
            limit: int = 10,
            batch_size: int = 5,
            scrapper_page_size: int = 5,
            scrapper_max_concurrent_requests: int = 5,
            scrapper_requests_per_second: int = 5
    ):
        """
        Saves top Github repositories to Clickhouse database.

        Parameters
        ----------
        access_token: str
            Github API access token to be used in requests.
        limit: int
            Maximum number of Github top repositories to fetch.
        batch_size: int
            Maximum number of repos to be saved to Clickhouse at once.
        scrapper_page_size: int
            Maximum number of repos to retrieve from Github API per request.
        scrapper_max_concurrent_requests: int
            Maximum concurrent requests allowed to be sent in the scrapper.
        scrapper_requests_per_second: int
            Maximum requests allowed for the scrapper to be sent in one second.
        """
        logger.info(
            "Fetching and saving Github top repositories to Clickhouse."
        )
        if not self._initialized:
            raise EnvironmentError(
                "Please use .init() method "
                "before using GithubClickhouse Saver."
            )
        repos_scrapper = GithubReposScrapper(
            access_token=access_token,
            max_concurrent_requests=scrapper_max_concurrent_requests,
            requests_per_second=scrapper_requests_per_second
        )
        async with repos_scrapper:
            repos_page = []
            current_save_task = None
            logger.debug("Iterating over top Github repositories")
            total_fetched = 0
            async for repo in repos_scrapper.iter_repositories(
                limit=limit,
                page_size=scrapper_page_size
            ):
                repos_page.append(repo)
                total_fetched += 1
                logger.debug(f"Added repo to the batch {repo}.")
                if len(repos_page) >= batch_size:
                    logger.debug("The batch is full.")
                    if current_save_task:
                        logger.debug(
                            "Waiting for the last saving task to finish"
                        )
                        await asyncio.gather(current_save_task)
                    logger.debug(f"Creating task to save batch: {repos_page}")
                    current_save_task = asyncio.create_task(
                        self._save_repos_to_clickhouse(repos_page)
                    )
                    repos_page = []
            logger.debug("Retrieving of top repos is finished.")
            if current_save_task:
                logger.debug("Waiting for the last save task to finish.")
                await asyncio.gather(current_save_task)
            if repos_page:
                logger.debug(
                    f"There is still unsaved repos "
                    f"(batch is not full). Saving it: {repos_page}"
                )
                await self._save_repos_to_clickhouse(repos_page)
            logger.info(
                f"{total_fetched} top repositories fetched "
                f"and saved to Clickhouse DB."
            )

    async def _save_repos_to_clickhouse(self, repos: list[Repository]):
        """
        Saves passed repositories objects to Clickhouse database.

        Parameters
        ----------
        repos: list[Repository]
            Objects to insert into the table.
        """
        logger.debug(f"Saving repos to clickhouse {repos}")
        repos_dtos, repos_positions_dtos, repos_commits_dtos = [], [], []
        for repo in repos:
            repo_dict = asdict(repo)
            repo_name = f"{repo.owner}/{repo.name}"
            position, _ = (
                repo_dict.pop("position"),
                repo_dict.pop("authors_commits_num_today")
            )
            repos_dtos.append(
                RepositoryDTO(
                    **repo_dict,
                    updated=datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
                )
            )
            repos_positions_dtos.append(
                RepositoriesPositionsDTO(
                    date=datetime.now(UTC).date(),
                    repo=repo_name,
                    position=position
                )
            )
            repos_commits_dtos.extend(
                RepositoriesAuthorsCommitsDTO(
                    date=datetime.now(UTC).date(),
                    repo=repo_name,
                    author=commit.author,
                    commits_num=commit.commits_num
                )
                for commit in repo.authors_commits_num_today
            )
        logger.debug(f"Repositories: {repos_dtos}")
        logger.debug(f"Repositories positions: {repos_positions_dtos}")
        logger.debug(f"Repositories authors commits: {repos_commits_dtos}")
        repos = (
            (self._repositories_repository, repos_dtos),
            (self._repositories_positions_repository, repos_positions_dtos),
            (self._repositories_authors_commits_repository, repos_commits_dtos)
        )
        tasks = []
        for repo, dtos in repos:
            if dtos:
                tasks.append(asyncio.create_task(repo.insert(dtos)))
        await asyncio.gather(*tasks)
