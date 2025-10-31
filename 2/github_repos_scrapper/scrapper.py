import asyncio
import time
from collections import Counter
from datetime import datetime, UTC, timedelta
from typing import Any, AsyncGenerator

from aiohttp import ClientSession, ClientError
from aiolimiter import AsyncLimiter

from github_repos_scrapper.logger import logger
from github_repos_scrapper.exceptions import GithubNotAvailableError
from github_repos_scrapper.utils import async_execute
from github_repos_scrapper.schemas import (
    RepositoryAuthorCommitsNum,
    Repository,
    GithubResponse
)


class GithubReposScrapper:
    """
    Class which scrapes top repositories in Github and returns info about them.
    It includes base info (name, owner etc.) and list of commit authors for
    the last 24 hours with number of their commits.
    """
    def __init__(
            self,
            access_token: str,
            base_url: str = "https://api.github.com",
            max_concurrent_requests: int = 5,
            requests_per_second: int = 5
    ):
        """
        Parameters
        ----------
        access_token: str
            Token to be used during requests to Github API.
            Usually acquired in Github developer settings.
        base_url: str
            Base url to Github API, without endpoint. You more likely shouldn't
            override it, provided you don't want to use another
            Github API host (proxy etc.)
        max_concurrent_requests: int
            Maximum number of requests executed concurrently (MCR).
        requests_per_second: int
            Maximum number of requests executed in one second.
        """
        if base_url.endswith("/"):
            base_url = base_url[:-1]
        self._session = None
        self._github_api_base_url = base_url
        self._max_concurrent_requests = max_concurrent_requests
        self._requests_semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._requests_limiter = AsyncLimiter(requests_per_second, 1)
        self.__access_token = access_token

    async def __aenter__(self):
        self.__create_client_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """
        This method had better be used at the end of your main script.
        It does async cleanup (close connections etc.)
        """
        if self._session:
            await self._session.close()

    async def get_repositories(self, limit: int = 5) -> list[Repository]:
        """
        Fetches top repositories from Github.

        Parameters
        ----------
        limit: int
            Maximum number of top repositories to be fetched.
            Cannot be more than 1000 (Github API limitations).

        Returns
        ----------
        list[Repository]
            List of objects representing repositories.
        """
        logger.info(f"Getting top {limit} repositories on Github")
        return [
            repository
            async for repository in self.iter_repositories(
                limit=limit,
                page_size=min(limit, 100)
            )
        ]

    async def iter_repositories(
            self,
            limit: int = 10,
            page_size: int = 5
    ) -> AsyncGenerator[Repository, None]:
        """
        Iterates over top repositories on Github.
        Use this to efficiently go over all repositories,
        without fetching all at once.

        Parameters
        ----------
        limit: int
            Maximum number of top repositories to be fetched.
            Cannot be more than 1000 (Github API limitations).
        page_size: int
            Maximum number of repositories to fetch in one request.
            Cannot be more than 100 (Github API limitations).

        Returns
        ----------
        Generator[Repository]
            Object to iterate over.
        """
        if limit > 1000:
            raise ValueError(
                "Github API doesn't allow to fetch "
                "more than 1000 repositories."
            )
        logger.debug(
            f"Iterating over top repositories "
            f"(limit {limit}, page_size: {page_size})"
        )
        for page in range(0, limit, page_size):
            repositories_page = await self._get_top_repositories(
                page=page,
                page_size=page_size
            )
            logger.debug(
                f"Retrieved repositories page of len {len(repositories_page)} "
                f"{repositories_page}"
            )
            repositories = await async_execute(
                self._process_raw_repository,
                [
                    (repo, i)
                    for i, repo in enumerate(repositories_page, 1+page)
                ],
                max_workers=self._max_concurrent_requests
            )
            for i, repository in enumerate(repositories, page):
                if i >= limit:
                    return
                yield repository


    async def _get_top_repositories(
            self,
            page: int = 0,
            page_size: int = 10
    ) -> list[dict[str, Any]]:
        """
        Makes a request to Github API search/repositories and returns
        a raw list of repositories.

        Parameters
        ----------
        page: int
            The number of page in repositories list.
        page_size: int
            Maximum number of repositories to be fetched in one page.

        Returns
        ----------
        list[dict[str, Any]]
            List of raw repositories.
        """
        if page_size > 100:
            raise ValueError("Github API doesn't allow to use page_size > 100")
        logger.debug(
            f"Getting repositories with page size {page_size} and page {page}"
        )
        response = await self.__make_request(
            endpoint="search/repositories",
            params={
                "q": "stars:>1",
                "sort": "stars",
                "order": "desc",
                "per_page": page_size,
                "page": page
            },
        )
        return response.data["items"]

    async def _get_repository_commits(
            self,
            owner: str,
            repo: str,
            days_span: int = 1
    ) -> list[dict[str, Any]]:
        """
        Fetches repository commits for the last days_span days.

        Parameters
        ----------
        owner: str
            Login of an owner of the repository.
        repo: str
            Name of the repository.
        days_span: int
            Number of days till today to fetch commits from.

        Returns
        ----------
        list[dict[str, Any]]
            List of raw commits objects.
        """
        _logger = logger.bind(owner=owner, repo=repo, days_span=days_span)
        date_since = datetime.now(UTC) - timedelta(days=days_span)
        _logger.debug(f"Getting repository commits since {date_since}")
        result, page = [], 1
        first_response = await self.__make_request(
            endpoint=f"repos/{owner}/{repo}/commits",
            params={"since": date_since.isoformat(), "page": page},
        )
        _logger.debug(f"First response getting commits: {first_response}")
        pages_to_process = []
        if first_response.links:
            page_url = first_response.links["last"]["url"]
            last_page_num = int(page_url.query["page"])
            pages_to_process = [
                (page_url.path, "GET", dict(page_url.query, page=page))
                for page in range(2, last_page_num + 1)
            ]
        _logger.debug(
            f"Pages to process after first response: {pages_to_process}"
        )
        results = first_response.data
        if not pages_to_process:
            return results
        results.extend(
            result
            for response in await async_execute(
                self.__make_request,
                pages_to_process,
                max_workers=self._max_concurrent_requests
            )
            for result in response.data
        )
        return results

    async def _process_raw_repository(
            self,
            repo: dict[str, Any],
            position: int
    ) -> Repository:
        """
        Converts raw repository dict object to Repository object,
        fetching and counting all commits.

        Parameters
        ----------
        repo: dict[str, Any]
            Raw repository object to process.
        position: int
            Position in the top of Github repositories.

        Returns
        ----------
        Repository
            Object of the repository with filled fields.
        """
        _logger = logger.bind(repo=repo["name"])
        _logger.debug(
            f"Processing raw repo object: {repo}, position: {position}"
        )
        commits = await self._get_repository_commits(
            owner=repo["owner"]["login"],
            repo=repo["name"]
        )
        _logger.debug(f"Commits retrieved. List: {commits}")
        counted_authors = Counter([
            (commit.get("author") or {}).get("id")
            or commit["commit"]["author"]["email"]
            for commit in commits
        ])
        _logger.debug(f"Counted authors: {counted_authors}")
        return Repository(
            name=repo["name"],
            owner=repo["owner"]["login"],
            position=position,
            stars=repo["stargazers_count"],
            watchers=repo["watchers_count"],
            forks=repo["forks_count"],
            language=repo["language"],
            authors_commits_num_today=[
                RepositoryAuthorCommitsNum(
                    author=author,
                    commits_num=count
                )
                for author, count in counted_authors.items()
            ]
        )

    def __create_client_session(self):
        """Creates aiohttp client session object when necessary."""
        self._session = ClientSession(
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {self.__access_token}",
            }
        )

    async def __make_request(
            self,
            endpoint: str,
            method: str = "GET",
            params: dict[str, Any] | None = None
    ) -> GithubResponse:
        """
        Makes a request to Github API.

        Parameters
        ----------
        endpoint: str
            Endpoint of the API to use.
        method: str
            HTTP request method to use (GET, POST, PUT, DELETE...).
        params: dict[str, Any] | None
            Query parameters to use with the request.

        Returns
        ----------
        GithubResponse
            Response of the Github API.
        """
        _logger = logger.bind(method=method, endpoint=endpoint, params=params)
        _logger.debug(f"Making a request")
        if endpoint.startswith("/"):
            endpoint = endpoint[1:]
        if not self._session:
            self.__create_client_session()
        async with self._requests_semaphore:
            _logger.debug("Passed _requests_semaphore")
            for attempt in range(5):
                try:
                    async with self._requests_limiter:
                        _logger.debug("Passed _requests_limiter")
                        async with self._session.request(
                            method=method,
                            url=f"{self._github_api_base_url}/{endpoint}",
                            params=params
                        ) as response:
                            if response.status < 400:
                                _logger.debug("Successful request.")
                                return GithubResponse(
                                    links=(
                                        dict(response.links)
                                        if response.links
                                        else None
                                    ),
                                    data=await response.json()
                                )
                            elif response.status == 401:
                                raise EnvironmentError(
                                    "Github API returned UNAUTHORIZED (401). "
                                    "Please check your access token."
                                )
                            elif (
                                response.status == 403
                                and (
                                    response.headers.get(
                                        "X-RateLimit-Remaining"
                                    ) == "0"
                                )
                            ):
                                reset_ts = int(
                                    response.headers["X-RateLimit-Reset"]
                                )
                                wait_for = max(0, reset_ts - time.time()) + 1
                                _logger.info(
                                    f"Rate limit hit. "
                                    f"Sleeping for {wait_for:.0f} seconds..."
                                )
                                await asyncio.sleep(wait_for)
                            else:
                                _logger.debug(
                                    f"Unexpected HTTP status response: "
                                    f"{response.status} {response.content}"
                                )
                except ClientError as error:
                    _logger.debug(
                        f"ClientError during Github API request: "
                        f"{method} {endpoint}, {params}."
                        f"{type(error)}, {error.args}"
                    )
                _logger.warning(
                    f"There was an error sending Github API request "
                    f"({attempt}). Making another one."
                )
                await asyncio.sleep(2 ** attempt)
            raise GithubNotAvailableError(
                "Couldn't get a response from Github API after 5 retries."
            )

