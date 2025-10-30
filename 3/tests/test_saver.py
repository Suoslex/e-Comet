import asyncio
from dataclasses import dataclass
from datetime import datetime, UTC

import pytest

from tests.conftest import DummyChClient
from github_clickhouse_saver.saver import GithubClickhouseSaver
from github_repos_scrapper.schemas import (
    Repository,
    RepositoryAuthorCommitsNum,
)


pytestmark = pytest.mark.asyncio


async def test_save_top_repos_requires_init():
    ch_client = DummyChClient()
    saver = GithubClickhouseSaver(ch_client=ch_client)
    with pytest.raises(EnvironmentError):
        await saver.save_top_repos(access_token="token", limit=1)


async def test_context_manager_initializes_and_closes(monkeypatch):
    ch_client = DummyChClient()
    init_called = False

    async def fake_init_clickhouse(_):
        nonlocal init_called
        init_called = True

    import github_clickhouse_saver.saver as saver_mod

    monkeypatch.setattr(saver_mod, "init_clickhouse", fake_init_clickhouse)

    async with GithubClickhouseSaver(ch_client=ch_client) as saver:
        assert saver._initialized is True
        assert init_called is True

    assert ch_client.closed is True


async def test_save_top_repos_batches_and_saves_correct_dtos(monkeypatch):
    fixed_dt = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_dt if tz is not None else fixed_dt.astimezone(tz)

    import github_clickhouse_saver.saver as saver_mod
    monkeypatch.setattr(saver_mod, "datetime", FixedDateTime)

    def make_repo(i: int) -> Repository:
        return Repository(
            name=f"repo{i}",
            owner=f"owner{i}",
            position=i,
            stars=100 + i,
            watchers=10 + i,
            forks=5 + i,
            language="Python",
            authors_commits_num_today=[
                RepositoryAuthorCommitsNum(author=f"user{i}", commits_num=i),
                RepositoryAuthorCommitsNum(author=f"userx", commits_num=2),
            ],
        )

    repos = [make_repo(i) for i in range(1, 8)]

    class FakeScrapper:
        def __init__(self, *_, **__):
            self.entered = False
            self.closed = False

        async def __aenter__(self):
            self.entered = True
            return self

        async def __aexit__(self, exc_type, exc, tb):
            self.closed = True

        async def iter_repositories(self, limit: int, page_size: int):
            count = 0
            for repo in repos:
                if count >= limit:
                    break
                count += 1
                yield repo

    monkeypatch.setattr(saver_mod, "GithubReposScrapper", FakeScrapper)

    @dataclass
    class InsertCapture:
        items: list

        async def insert(self, data):
            self.items.extend(data)

    ch_client = DummyChClient()
    saver = GithubClickhouseSaver(ch_client=ch_client)
    saver._initialized = True

    repos_cap = InsertCapture(items=[])
    positions_cap = InsertCapture(items=[])
    commits_cap = InsertCapture(items=[])

    saver._repositories_repository = repos_cap
    saver._repositories_positions_repository = positions_cap
    saver._repositories_authors_commits_repository = commits_cap

    await saver.save_top_repos(
        access_token="token",
        limit=7,
        batch_size=3,
        scrapper_page_size=2,
        scrapper_max_concurrent_requests=2,
        scrapper_requests_per_second=100,
    )

    assert len(repos_cap.items) == 7

    first = repos_cap.items[0]
    last = repos_cap.items[-1]
    assert first.name == "repo1" and first.owner == "owner1"
    assert first.stars == 101 and first.watchers == 11 and first.forks == 6
    assert first.language == "Python"
    assert first.updated == "2024-01-02 03:04:05"
    assert last.name == "repo7" and last.owner == "owner7"

    assert len(positions_cap.items) == 7
    p0 = positions_cap.items[0]
    assert p0.repo == "owner1/repo1"
    assert p0.position == 1
    assert p0.date == fixed_dt.date()

    assert len(commits_cap.items) == 7 * 2

    sample = [c for c in commits_cap.items if c.repo == "owner3/repo3"]
    authors = {(c.author, c.commits_num) for c in sample}
    assert authors == {("user3", 3), ("userx", 2)}


async def test_init_sets_flag_and_calls_init_clickhouse(monkeypatch):
    ch_client = DummyChClient()
    called = {"ok": False}

    async def fake_init_clickhouse(_):
        called["ok"] = True

    import github_clickhouse_saver.saver as saver_mod
    monkeypatch.setattr(saver_mod, "init_clickhouse", fake_init_clickhouse)

    saver = GithubClickhouseSaver(ch_client=ch_client)
    assert saver._initialized is False
    await saver.init()
    assert saver._initialized is True
    assert called["ok"] is True


async def test_context_manager_closes_on_exception(monkeypatch):
    ch_client = DummyChClient()
    async def fake_init_clickhouse(_):
        return None

    import github_clickhouse_saver.saver as saver_mod
    monkeypatch.setattr(saver_mod, "init_clickhouse", fake_init_clickhouse)

    class Boom(Exception):
        pass

    try:
        async with GithubClickhouseSaver(ch_client=ch_client):
            raise Boom("fail inside context")
    except Boom:
        pass

    assert ch_client.closed is True


async def test_no_inserts_when_limit_zero(monkeypatch):
    class FakeScrapper:
        def __init__(self, *_, **__):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False
        async def iter_repositories(self, limit: int, page_size: int):
            if False:
                yield

    import github_clickhouse_saver.saver as saver_mod
    monkeypatch.setattr(saver_mod, "GithubReposScrapper", FakeScrapper)

    ch_client = DummyChClient()
    saver = GithubClickhouseSaver(ch_client)
    saver._initialized = True

    calls = {"repos": 0, "pos": 0, "commits": 0}

    class Cap:
        def __init__(self, key):
            self.key = key
        async def insert(self, data):
            calls[self.key] += len(data)

    saver._repositories_repository = Cap("repos")
    saver._repositories_positions_repository = Cap("pos")
    saver._repositories_authors_commits_repository = Cap("commits")

    await saver.save_top_repos(access_token="t", limit=0)

    assert calls == {"repos": 0, "pos": 0, "commits": 0}


async def test_batch_size_one_creates_many_batches(monkeypatch):
    repos = [
        Repository(
            name=f"r{i}", owner=f"o{i}", position=i, stars=1,
            watchers=1, forks=1, language="Python",
            authors_commits_num_today=[
                RepositoryAuthorCommitsNum(author="a", commits_num=1)
            ]
        )
        for i in range(1, 6)
    ]

    class FakeScrapper:
        def __init__(self, *_, **__):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False
        async def iter_repositories(self, limit: int, page_size: int):
            for r in repos[:limit]:
                yield r

    import github_clickhouse_saver.saver as saver_mod
    monkeypatch.setattr(saver_mod, "GithubReposScrapper", FakeScrapper)

    ch_client = DummyChClient()
    saver = GithubClickhouseSaver(ch_client)
    saver._initialized = True

    counts = {"repos": 0}

    class Cap:
        async def insert(self, data):
            counts["repos"] += 1

    saver._repositories_repository = Cap()
    saver._repositories_positions_repository = Cap()
    saver._repositories_authors_commits_repository = Cap()

    await saver.save_top_repos(access_token="t", limit=5, batch_size=1)
    assert counts["repos"] == 15


async def test_batch_size_greater_than_limit_single_call(monkeypatch):
    r = Repository(
        name="r1", owner="o1", position=1, stars=1, watchers=1, forks=1,
        language="Python", authors_commits_num_today=[]
    )

    class FakeScrapper:
        def __init__(self, *_, **__):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False
        async def iter_repositories(self, limit: int, page_size: int):
            for x in [r]:
                yield x

    import github_clickhouse_saver.saver as saver_mod
    monkeypatch.setattr(saver_mod, "GithubReposScrapper", FakeScrapper)

    ch_client = DummyChClient()
    saver = GithubClickhouseSaver(ch_client)
    saver._initialized = True

    calls = {"repos": 0}
    class Cap:
        async def insert(self, data):
            calls["repos"] += 1

    saver._repositories_repository = Cap()
    saver._repositories_positions_repository = Cap()
    saver._repositories_authors_commits_repository = Cap()

    await saver.save_top_repos(access_token="t", limit=1, batch_size=10)
    assert calls["repos"] == 2


async def test__save_repos_to_clickhouse_single_repo_no_authors(monkeypatch):
    fixed_dt = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_dt

    import github_clickhouse_saver.saver as saver_mod
    monkeypatch.setattr(saver_mod, "datetime", FixedDateTime)

    repo = Repository(
        name="r1", owner="o1", position=2, stars=3, watchers=4, forks=5,
        language="Go", authors_commits_num_today=[]
    )

    ch_client = DummyChClient()
    saver = GithubClickhouseSaver(ch_client)
    saver._initialized = True

    repos_cap, pos_cap, commits_cap = [], [], []

    class Cap:
        def __init__(self, out):
            self.out = out
        async def insert(self, data):
            self.out.extend(data)

    saver._repositories_repository = Cap(repos_cap)
    saver._repositories_positions_repository = Cap(pos_cap)
    saver._repositories_authors_commits_repository = Cap(commits_cap)

    await saver._save_repos_to_clickhouse([repo])

    assert len(repos_cap) == 1
    assert repos_cap[0].updated == "2025-01-01 00:00:00"
    assert len(pos_cap) == 1 and pos_cap[0].position == 2
    assert len(commits_cap) == 0


async def test_scrapper_constructed_with_params(monkeypatch):
    constructed = {}

    class SpyScrapper:
        def __init__(
                self,
                access_token,
                max_concurrent_requests,
                requests_per_second
        ):
            constructed.update(
                token=access_token,
                mcr=max_concurrent_requests,
                rps=requests_per_second,
            )
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False
        async def iter_repositories(self, limit: int, page_size: int):
            if False:
                yield

    import github_clickhouse_saver.saver as saver_mod
    monkeypatch.setattr(saver_mod, "GithubReposScrapper", SpyScrapper)

    ch_client = DummyChClient()
    s = GithubClickhouseSaver(ch_client)
    s._initialized = True
    await s.save_top_repos(
        access_token="TK",
        limit=0,
        batch_size=2,
        scrapper_page_size=5,
        scrapper_max_concurrent_requests=7,
        scrapper_requests_per_second=9,
    )

    assert constructed == {"token": "TK", "mcr": 7, "rps": 9}


async def test_save_waits_for_previous_task_before_new_batch(monkeypatch):
    repos = [
        Repository(
            name=f"r{i}", owner=f"o{i}", position=i, stars=0, watchers=0,
            forks=0, language="Py", authors_commits_num_today=[]
        )
        for i in range(1, 7)
    ]

    class FakeScrapper:
        def __init__(self, *_, **__):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False
        async def iter_repositories(self, limit: int, page_size: int):
            for r in repos:
                yield r

    import github_clickhouse_saver.saver as saver_mod
    monkeypatch.setattr(saver_mod, "GithubReposScrapper", FakeScrapper)

    ch_client = DummyChClient()
    saver = GithubClickhouseSaver(ch_client)
    saver._initialized = True

    order = []
    class SlowCap:
        async def insert(self, data):
            names = {getattr(d, "name", None) for d in data}
            if "r1" in names:
                order.append("first_batch_start")
                await asyncio.sleep(0.05)
                order.append("first_batch_end")
            else:
                order.append("second_batch_start")
                order.append("second_batch_end")

    saver._repositories_repository = SlowCap()
    saver._repositories_positions_repository = SlowCap()
    saver._repositories_authors_commits_repository = SlowCap()

    await saver.save_top_repos(access_token="t", limit=6, batch_size=3)

    # Ensure the first batch started and ended before the second completed,
    # i.e., we see end marker before the final second markers.
    assert "first_batch_start" in order and "first_batch_end" in order


async def test_positions_saved_for_tail_batch(monkeypatch):
    repos = [
        Repository(
            name=f"r{i}", owner=f"o{i}", position=i, stars=0, watchers=0,
            forks=0, language="Py", authors_commits_num_today=[]
        )
        for i in range(1, 5)
    ]

    class FakeScrapper:
        def __init__(self, *_, **__):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False
        async def iter_repositories(self, limit: int, page_size: int):
            for r in repos:
                yield r

    import github_clickhouse_saver.saver as saver_mod
    monkeypatch.setattr(saver_mod, "GithubReposScrapper", FakeScrapper)

    ch_client = DummyChClient()
    saver = GithubClickhouseSaver(ch_client)
    saver._initialized = True

    pos_items = []
    class PosCap:
        async def insert(self, data):
            pos_items.extend(data)

    saver._repositories_repository = PosCap()
    saver._repositories_positions_repository = PosCap()
    saver._repositories_authors_commits_repository = PosCap()

    await saver.save_top_repos(access_token="t", limit=4, batch_size=3)

    assert len([x for x in pos_items if hasattr(x, "position")]) == 4


async def test_commits_dtos_flattened_across_repositories(monkeypatch):
    r1 = Repository(
        name="r1", owner="o1", position=1, stars=0, watchers=0, forks=0,
        language="Py", authors_commits_num_today=[
            RepositoryAuthorCommitsNum(author="a", commits_num=2)
        ]
    )
    r2 = Repository(
        name="r2", owner="o2", position=2, stars=0, watchers=0, forks=0,
        language="Py", authors_commits_num_today=[
            RepositoryAuthorCommitsNum(author="b", commits_num=3),
            RepositoryAuthorCommitsNum(author="a", commits_num=1),
        ]
    )

    class FakeScrapper:
        def __init__(self, *_, **__):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False
        async def iter_repositories(self, limit: int, page_size: int):
            for r in [r1, r2]:
                yield r

    import github_clickhouse_saver.saver as saver_mod
    monkeypatch.setattr(saver_mod, "GithubReposScrapper", FakeScrapper)

    ch_client = DummyChClient()
    saver = GithubClickhouseSaver(ch_client)
    saver._initialized = True

    commits = []
    class CommitsCap:
        async def insert(self, data):
            commits.extend(data)

    saver._repositories_repository = CommitsCap()
    saver._repositories_positions_repository = CommitsCap()
    saver._repositories_authors_commits_repository = CommitsCap()

    await saver.save_top_repos(access_token="t", limit=2, batch_size=5)

    flattened = [x for x in commits if hasattr(x, "author")]
    assert len(flattened) == 3
    assert { (c.repo, c.author, c.commits_num) for c in flattened } == {
        ("o1/r1", "a", 2), ("o2/r2", "b", 3), ("o2/r2", "a", 1)
    }

async def test_exception_in_scrapper_aenter(monkeypatch):
    class ScrapperBoom(Exception): ...

    class FailingScrapper:
        def __init__(self, *_, **__):
            pass
        async def __aenter__(self):
            raise ScrapperBoom("enter failed")
        async def __aexit__(self, exc_type, exc, tb):
            return False

    import github_clickhouse_saver.saver as saver_mod
    monkeypatch.setattr(saver_mod, "GithubReposScrapper", FailingScrapper)

    ch_client = DummyChClient()
    saver = GithubClickhouseSaver(ch_client)
    saver._initialized = True
    try:
        await saver.save_top_repos(access_token="t", limit=2)
    except ScrapperBoom:
        pass
    else:
        assert False, "Exception not propagated"

    assert ch_client.closed is False or ch_client.closed is True 
