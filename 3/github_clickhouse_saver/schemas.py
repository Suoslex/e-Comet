from dataclasses import dataclass
from datetime import date


@dataclass
class RepositoryDTO:
    name: str
    owner: str
    stars: int
    watchers: int
    forks: int
    language: str
    updated: str


@dataclass
class RepositoriesAuthorsCommitsDTO:
    date: date
    repo: str
    author: str
    commits_num: int


@dataclass
class RepositoriesPositionsDTO:
    date: date
    repo: str
    position: int
