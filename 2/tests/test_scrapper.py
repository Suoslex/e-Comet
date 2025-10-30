import time
import asyncio

import pytest
from aiohttp import ClientError

from github_repos_scrapper.schemas import Repository
from github_repos_scrapper.schemas import GithubResponse
from github_repos_scrapper.scrapper import GithubReposScrapper
from github_repos_scrapper.exceptions import GithubNotAvailableError

pytestmark = pytest.mark.asyncio


async def test_scrapper_initializes_properly():
    try:
        GithubReposScrapper("test_access_token")
    except Exception as error:
        pytest.fail(
            f"Could not initialize scrapper properly "
            f"(got exception {error}, {error.args})"
        )


async def test_scrapper_cannot_be_initialized_without_token():
    with pytest.raises(TypeError):
        GithubReposScrapper()

@pytest.mark.parametrize(
    'params',
    [
        {'max_concurrent_requests': 10},
        {'requests_per_second': 10},
        {'max_concurrent_requests': 20, 'requests_per_second': 20},
    ]
)
async def test_scrapper_can_be_initialized_with_params(params):
    try:
        GithubReposScrapper("test_access_token", **params)
    except Exception as error:
        pytest.fail(
            f"Could not initialize scrapper properly with params {params} "
            f"(got exception {error}, {error.args})"
        )

async def test_scrapper_opens_and_closes_session_in_context_manager_properly():
    async with GithubReposScrapper("test") as scrapper:
        assert scrapper._session.closed is False
        pass
    assert scrapper._session.closed is True


async def test_scrapper_closes_session_on_close():
    scrapper = GithubReposScrapper("test")
    scrapper._GithubReposScrapper__create_client_session()
    assert scrapper._session.closed is False
    await scrapper.close()
    assert scrapper._session.closed is True



async def test_get_repositories_collects_from_iterator(monkeypatch):
    async def fake_iter(limit=5, page_size=1):
        for x in ["A", "B", "C"]:
            yield x

    scrapper = GithubReposScrapper("test")
    monkeypatch.setattr(scrapper, "iter_repositories", fake_iter)
    try:
        result = await scrapper.get_repositories(limit=3)
        assert result == ["A", "B", "C"]
    finally:
        await scrapper.close()


async def test_iter_repositories_uses_pagination_and_yields_results(
        monkeypatch
):
    scrapper = GithubReposScrapper("test")
    repos_pages = [
        [
            {
                "name": "r1",
                "owner": {"login": "o1"},
                "stargazers_count": 0,
                "watchers_count": 0,
                "forks_count": 0,
                "language": "py"
            },
            {
                "name": "r2",
                "owner": {"login": "o2"},
                "stargazers_count": 0,
                "watchers_count": 0,
                "forks_count": 0,
                "language": "py"
            },
        ],
        [
            {
                "name": "r3",
                "owner": {"login": "o3"},
                "stargazers_count": 0,
                "watchers_count": 0,
                "forks_count": 0,
                "language": "py"
            },
            {
                "name": "r4",
                "owner": {"login": "o4"},
                "stargazers_count": 0,
                "watchers_count": 0,
                "forks_count": 0,
                "language": "py"
            },
        ],
    ]

    calls = {"page": []}

    async def fake_get_top_repositories(page=0, page_size=100):
        calls["page"].append(page)
        return repos_pages[page // page_size]

    async def fake_async_execute(func, args_list, max_workers=10):
        return [f"P{idx+1}" for idx, _ in enumerate(args_list)]

    monkeypatch.setattr(
        scrapper,
        "_get_top_repositories",
        fake_get_top_repositories
    )
    monkeypatch.setattr(
        "github_repos_scrapper.scrapper.async_execute",
        fake_async_execute
    )

    try:
        results = []
        async for repo in scrapper.iter_repositories(limit=4, page_size=2):
            results.append(repo)
        assert results == ["P1", "P2", "P1", "P2"]
        assert calls["page"] == [0, 2]
    finally:
        await scrapper.close()


async def test_get_top_repositories_raises_on_page_size_over_1000():
    scrapper = GithubReposScrapper("test")
    try:
        with pytest.raises(ValueError):
            await scrapper._get_top_repositories(page=0, page_size=1001)
    finally:
        await scrapper.close()


async def test_get_top_repositories_calls_make_request_and_returns_items(
        monkeypatch
):
    scrapper = GithubReposScrapper("test")

    async def fake_make_request(endpoint, method="GET", params=None):
        assert endpoint == "search/repositories"
        assert method == "GET"
        assert "per_page" in params and "page" in params
        return GithubResponse(links=None, data={"items": [1, 2, 3]})

    monkeypatch.setattr(
        scrapper,
        "_GithubReposScrapper__make_request",
        fake_make_request
    )
    try:
        res = await scrapper._get_top_repositories(page=3, page_size=10)
        assert res == [1, 2, 3]
    finally:
        await scrapper.close()


async def test_make_request_success_path(monkeypatch):
    scrapper = GithubReposScrapper("test")

    class DummyResp:
        def __init__(self):
            self.status = 200
            self.links = {"next": {"url": object()}}

        async def json(self):
            return {"ok": True}

        @property
        def headers(self):
            return {}

        @property
        def content(self):
            return b""

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class DummySession:
        def request(self, method, url, params=None):
            DummySession.captured = {
                "method": method,
                "url": url,
                "params": params
            }
            return DummyResp()

    class NoopCtx:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(scrapper, "_session", DummySession())
    monkeypatch.setattr(scrapper, "_requests_semaphore", NoopCtx())
    monkeypatch.setattr(scrapper, "_requests_limiter", NoopCtx())

    resp = await scrapper._GithubReposScrapper__make_request(
        "foo/bar",
        params={"x": 1}
    )
    assert isinstance(resp, GithubResponse)
    assert DummySession.captured["url"].endswith("/foo/bar")


async def test_make_request_strips_leading_slash(monkeypatch):
    scrapper = GithubReposScrapper("test")

    class DummyResp:
        status = 200
        links = None

        async def json(self):
            return {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    captured = {}

    class DummySession:
        def request(self, method, url, params=None):
            captured["url"] = url
            return DummyResp()

    class NoopCtx:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(scrapper, "_session", DummySession())
    monkeypatch.setattr(scrapper, "_requests_semaphore", NoopCtx())
    monkeypatch.setattr(scrapper, "_requests_limiter", NoopCtx())

    await scrapper._GithubReposScrapper__make_request(
        "/with/leading",
        params={}
    )
    assert "//" not in captured["url"][captured["url"].find("://") + 3:]


async def test_make_request_handles_rate_limit_and_retries(monkeypatch):
    scrapper = GithubReposScrapper("test")

    sleeps = []

    async def fake_sleep(n):
        sleeps.append(n)

    class RLResp:
        def __init__(self, status, headers):
            self.status = status
            self.links = None
            self._headers = headers

        @property
        def headers(self):
            return self._headers

        @property
        def content(self):
            return b""

        async def json(self):
            return {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    calls = {"count": 0}

    class DummySession:
        def request(self, method, url, params=None):
            calls["count"] += 1
            if calls["count"] == 1:
                return RLResp(
                    403,
                    {
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": str(int(time.time()))
                    }
                )
            return RLResp(200, {})

    class NoopCtx:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr("asyncio.sleep", fake_sleep)
    monkeypatch.setattr(scrapper, "_session", DummySession())
    monkeypatch.setattr(scrapper, "_requests_semaphore", NoopCtx())
    monkeypatch.setattr(scrapper, "_requests_limiter", NoopCtx())

    await scrapper._GithubReposScrapper__make_request("x")
    assert calls["count"] == 2
    assert any(s >= 1 for s in sleeps)


async def test_make_request_retries_and_raises_after_5(monkeypatch):
    scrapper = GithubReposScrapper("test")

    async def fake_sleep(n):
        pass

    class BadResp:
        async def __aenter__(self):
            raise ClientError("boom")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class DummySession:
        def request(self, method, url, params=None):
            return BadResp()

    class NoopCtx:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr("asyncio.sleep", fake_sleep)
    monkeypatch.setattr(scrapper, "_session", DummySession())
    monkeypatch.setattr(scrapper, "_requests_semaphore", NoopCtx())
    monkeypatch.setattr(scrapper, "_requests_limiter", NoopCtx())

    with pytest.raises(Exception):
        await scrapper._GithubReposScrapper__make_request("x")

async def test_get_repository_commits_returns_first_page_only(monkeypatch):
    scrapper = GithubReposScrapper("test")

    async def fake_make_request(endpoint, method="GET", params=None):
        assert endpoint.startswith("repos/")
        return GithubResponse(links=None, data=[1, 2])

    monkeypatch.setattr(
        scrapper,
        "_GithubReposScrapper__make_request",
        fake_make_request
    )
    res = await scrapper._get_repository_commits("o", "r")
    assert res == [1, 2]


async def test_get_repository_commits_aggregates_all_pages(monkeypatch):
    class DummyUrl:
        def __init__(self, path, query):
            self.path = path
            self.query = query

    scrapper = GithubReposScrapper("test")

    first = GithubResponse(
        links={"last": {"url": DummyUrl("repos/o/r/commits", {"page": 3})}},
        data=[1, 2],
    )

    async def fake_first(endpoint, method="GET", params=None):
        return first

    async def fake_async_execute(func, args_list, max_workers=10):
        pages = []
        for (endpoint, method, params) in args_list:
            pages.append(GithubResponse(links=None, data=[params["page"]]))
        return pages

    monkeypatch.setattr(
        scrapper,
        "_GithubReposScrapper__make_request",
        fake_first)
    monkeypatch.setattr(
        "github_repos_scrapper.scrapper.async_execute",
        fake_async_execute
    )

    res = await scrapper._get_repository_commits("o", "r")
    assert res == [1, 2, 2, 3]


async def test_process_raw_repository_counts_authors(monkeypatch):
    scrapper = GithubReposScrapper("test")

    async def fake_get_commits(owner, repo, days_span=1):
        return [
            {"author": {"id": "u1"}, "commit": {"author": {"email": "a@a"}}},
            {"author": {"id": "u1"}, "commit": {"author": {"email": "a@a"}}},
            {"author": None, "commit": {"author": {"email": "b@b"}}},
        ]

    monkeypatch.setattr(scrapper, "_get_repository_commits", fake_get_commits)

    raw = {
        "name": "r",
        "owner": {"login": "o"},
        "stargazers_count": 10,
        "watchers_count": 20,
        "forks_count": 3,
        "language": "py",
    }

    repo = await scrapper._process_raw_repository(raw, 5)
    assert isinstance(repo, Repository)
    counts = {a.author: a.commits_num for a in repo.authors_commits_num_today}
    assert counts == {"u1": 2, "b@b": 1}


async def test_iter_repositories_uses_configured_max_workers(monkeypatch):
    scrapper = GithubReposScrapper("test", max_concurrent_requests=7)

    async def fake_get_top_repositories(page=0, page_size=100):
        return []

    captured = {"max_workers": None}

    async def fake_async_execute(func, args_list, max_workers=10):
        captured["max_workers"] = max_workers
        return []

    monkeypatch.setattr(
        scrapper,
        "_get_top_repositories",
        fake_get_top_repositories
    )
    monkeypatch.setattr(
        "github_repos_scrapper.scrapper.async_execute",
        fake_async_execute
    )

    async for _ in scrapper.iter_repositories(limit=1, page_size=1):
        pass
    assert captured["max_workers"] == 7


async def test_iter_repositories_partial_last_page(monkeypatch):
    scrapper = GithubReposScrapper("test")

    async def fake_get_top_repositories(page=0, page_size=2):
        if page == 0:
            return [
                {
                    "name": "r1",
                    "owner": {"login": "o1"},
                    "stargazers_count": 0,
                    "watchers_count": 0,
                    "forks_count": 0,
                    "language": "py"
                },
                {
                    "name": "r2",
                    "owner": {"login": "o2"},
                    "stargazers_count": 0,
                    "watchers_count": 0,
                    "forks_count": 0,
                    "language": "py"
                },
            ]
        if page == 2:
            return [
                {
                    "name": "r3",
                    "owner": {"login": "o3"},
                    "stargazers_count": 0,
                    "watchers_count": 0,
                    "forks_count": 0,
                    "language": "py"
                },
                {
                    "name": "r4",
                    "owner": {"login": "o4"},
                    "stargazers_count": 0,
                    "watchers_count": 0,
                    "forks_count": 0,
                    "language": "py"
                },
            ]
        if page == 4:
            return [
                {
                    "name": "r5",
                    "owner": {"login": "o5"},
                    "stargazers_count": 0,
                    "watchers_count": 0,
                    "forks_count": 0,
                    "language": "py"
                },
            ]
        return []

    async def fake_async_execute(func, args_list, max_workers=10):
        return [f"P{idx+1}" for idx, _ in enumerate(args_list)]

    monkeypatch.setattr(
        scrapper,
        "_get_top_repositories",
        fake_get_top_repositories
    )
    monkeypatch.setattr(
        "github_repos_scrapper.scrapper.async_execute",
        fake_async_execute
    )

    results = []
    async for item in scrapper.iter_repositories(limit=5, page_size=2):
        results.append(item)
    assert results == ["P1", "P2", "P1", "P2", "P1"]


async def test_iter_repositories_with_zero_limit(monkeypatch):
    scrapper = GithubReposScrapper("test")

    called = {"get": False}

    async def fake_get_top_repositories(page=0, page_size=1):
        called["get"] = True
        return []

    monkeypatch.setattr(
        scrapper,
        "_get_top_repositories",
        fake_get_top_repositories
    )
    results = []
    async for item in scrapper.iter_repositories(limit=0, page_size=1):
        results.append(item)
    assert results == []
    assert called["get"] is False


async def test_iter_repositories_with_invalid_page_size_raises():
    scrapper = GithubReposScrapper("test")
    with pytest.raises(ValueError):
        async for _ in scrapper.iter_repositories(limit=1, page_size=0):
            pass


async def test_get_repository_commits_empty_first_page(monkeypatch):
    scrapper = GithubReposScrapper("test")

    async def fake_make_request(endpoint, method="GET", params=None):
        return GithubResponse(links=None, data=[])

    monkeypatch.setattr(
        scrapper,
        "_GithubReposScrapper__make_request",
        fake_make_request
    )
    res = await scrapper._get_repository_commits("o", "r")
    assert res == []


async def test_get_repository_commits_missing_last_link(monkeypatch):
    scrapper = GithubReposScrapper("test")

    first = GithubResponse(links={}, data=[1])

    async def fake_first(endpoint, method="GET", params=None):
        return first

    monkeypatch.setattr(
        scrapper,
        "_GithubReposScrapper__make_request",
        fake_first
    )
    res = await scrapper._get_repository_commits("o", "r")
    assert res == [1]

async def test_make_request_retries_on_5xx_then_succeeds(monkeypatch):
    scrapper = GithubReposScrapper("test")

    class Resp:
        def __init__(self, status, data=None, headers=None):
            self.status = status
            self.links = None
            self._data = data or {}
            self._headers = headers or {}

        @property
        def headers(self):
            return self._headers

        @property
        def content(self):
            return b""

        async def json(self):
            return self._data

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    calls = {"n": 0}

    class Sess:
        def request(self, method, url, params=None):
            calls["n"] += 1
            if calls["n"] < 2:
                return Resp(500)
            return Resp(200, {"ok": True})

    class Noop:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    original = scrapper._session
    monkeypatch.setattr(scrapper, "_session", Sess())
    monkeypatch.setattr(scrapper, "_requests_semaphore", Noop())
    monkeypatch.setattr(scrapper, "_requests_limiter", Noop())

    resp = await scrapper._GithubReposScrapper__make_request("e")
    assert isinstance(resp, GithubResponse)
    assert calls["n"] >= 2

async def test_client_session_headers_on_init(monkeypatch):
    captured = {}

    class FakeSession:
        def __init__(self, headers=None):
            captured["headers"] = headers
            self.closed = False

        async def close(self):
            self.closed = True

    import github_repos_scrapper.scrapper as mod
    monkeypatch.setattr(mod, "ClientSession", FakeSession)

    token = "abc123"
    s = GithubReposScrapper(token)
    s._GithubReposScrapper__create_client_session()
    assert captured["headers"]["Authorization"] == f"Bearer {token}"
    assert captured["headers"]["Accept"].startswith(
        "application/vnd.github.v3"
    )

async def test_double_close_after_context_exit():
    async with GithubReposScrapper("t") as s:
        assert s._session.closed is False
    await s.close()
    assert s._session.closed is True


async def test_concurrency_cap_with_custom_semaphore(monkeypatch):
    scrapper = GithubReposScrapper("t", max_concurrent_requests=3)

    class Counter:
        active = 0
        max_seen = 0

    class CountingResp:
        status = 200
        links = None

        async def json(self):
            await asyncio.sleep(0)
            return {}

        async def __aenter__(self):
            Counter.active += 1
            Counter.max_seen = max(Counter.max_seen, Counter.active)
            await asyncio.sleep(0)
            return self

        async def __aexit__(self, exc_type, exc, tb):
            Counter.active -= 1
            return False

    class Sess:
        def request(self, method, url, params=None):
            return CountingResp()

    class NoopLimiter:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(scrapper, "_session", Sess())
    monkeypatch.setattr(scrapper, "_requests_limiter", NoopLimiter())

    await asyncio.gather(*[
        scrapper._GithubReposScrapper__make_request("e") for _ in range(10)
    ])
    assert Counter.max_seen <= 3


async def test_get_top_repositories_builds_expected_params_q_sort_order(
        monkeypatch
):
    scrapper = GithubReposScrapper("test")

    captured = {"endpoint": None, "params": None}

    async def fake_make_request(endpoint, method="GET", params=None):
        captured["endpoint"] = endpoint
        captured["params"] = params
        return GithubResponse(links=None, data={"items": []})

    monkeypatch.setattr(
        scrapper,
        "_GithubReposScrapper__make_request",
        fake_make_request
    )
    await scrapper._get_top_repositories(page=7, page_size=42)
    assert captured["endpoint"] == "search/repositories"
    assert captured["params"]["q"] == "stars:>1"
    assert captured["params"]["sort"] == "stars"
    assert captured["params"]["order"] == "desc"
    assert captured["params"]["per_page"] == 42
    assert captured["params"]["page"] == 7


async def test_make_request_retries_on_404_then_raises(monkeypatch):
    scrapper = GithubReposScrapper("x")

    class Resp:
        def __init__(self, status):
            self.status = status
            self.links = None

        @property
        def headers(self):
            return {}

        @property
        def content(self):
            return b""

        async def json(self):
            return {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class Sess:
        def request(self, method, url, params=None):
            return Resp(404)

    class Noop:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    original_sleep = asyncio.sleep
    async def tiny_sleep(n):
        await original_sleep(0)
    monkeypatch.setattr("asyncio.sleep", tiny_sleep)
    monkeypatch.setattr(scrapper, "_session", Sess())
    monkeypatch.setattr(scrapper, "_requests_semaphore", Noop())
    monkeypatch.setattr(scrapper, "_requests_limiter", Noop())

    with pytest.raises(GithubNotAvailableError):
        await scrapper._GithubReposScrapper__make_request("e")


async def test_make_request_exponential_backoff(monkeypatch):
    scrapper = GithubReposScrapper("x")

    sleeps = []

    async def fake_sleep(n):
        sleeps.append(n)

    class Resp:
        def __init__(self, status):
            self.status = status
            self.links = None

        @property
        def headers(self):
            return {}

        @property
        def content(self):
            return b""

        async def json(self):
            return {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class Sess:
        def __init__(self):
            self.calls = 0

        def request(self, method, url, params=None):
            self.calls += 1
            return Resp(500)

    class Noop:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr("asyncio.sleep", fake_sleep)
    monkeypatch.setattr(scrapper, "_session", Sess())
    monkeypatch.setattr(scrapper, "_requests_semaphore", Noop())
    monkeypatch.setattr(scrapper, "_requests_limiter", Noop())

    with pytest.raises(Exception):
        await scrapper._GithubReposScrapper__make_request("e")

    assert sleeps[:4] == [1, 2, 4, 8]


async def test_get_repository_commits_last_page_one(monkeypatch):
    class DummyUrl:
        def __init__(self, path, query):
            self.path = path
            self.query = query

    scrapper = GithubReposScrapper("test")

    first = GithubResponse(
        links={"last": {"url": DummyUrl("repos/o/r/commits", {"page": 1})}},
        data=["only-page"],
    )

    async def fake_first(endpoint, method="GET", params=None):
        return first

    monkeypatch.setattr(
        scrapper,
        "_GithubReposScrapper__make_request",
        fake_first
    )
    res = await scrapper._get_repository_commits("o", "r")
    assert res == ["only-page"]


async def test_process_raw_repository_sets_fields_correctly(monkeypatch):
    scrapper = GithubReposScrapper("test")

    async def fake_get_commits(owner, repo, days_span=1):
        return []

    monkeypatch.setattr(scrapper, "_get_repository_commits", fake_get_commits)

    raw = {
        "name": "lib",
        "owner": {"login": "me"},
        "stargazers_count": 5,
        "watchers_count": 9,
        "forks_count": 2,
        "language": "py",
    }
    repo = await scrapper._process_raw_repository(raw, 11)
    assert isinstance(repo, Repository)
    assert repo.name == "lib"
    assert repo.owner == "me"
    assert repo.position == 11
    assert repo.stars == 5
    assert repo.watchers == 9
    assert repo.forks == 2
    assert repo.language == "py"
    assert repo.authors_commits_num_today == []


async def test_make_request_base_url_with_trailing_slash(monkeypatch):
    scrapper = GithubReposScrapper("t", base_url="https://api.github.com/")

    captured = {"url": None}

    class Resp:
        status = 200
        links = None

        async def json(self):
            return {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class Sess:
        def request(self, method, url, params=None):
            captured["url"] = url
            return Resp()

    class Noop:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(scrapper, "_session", Sess())
    monkeypatch.setattr(scrapper, "_requests_semaphore", Noop())
    monkeypatch.setattr(scrapper, "_requests_limiter", Noop())

    await scrapper._GithubReposScrapper__make_request("repos/test")
    path_part = captured["url"][captured["url"].find("://") + 3:]
    assert "//" not in path_part

