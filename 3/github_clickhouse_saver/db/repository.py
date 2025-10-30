from abc import ABC, abstractmethod
from dataclasses import asdict

from aiochclient import ChClient


class Repository[T](ABC):
    """Base abstract class for repositories."""
    @abstractmethod
    async def insert(self, data: list[T]):
        """
        Insert new rows into the repository.
        
        Parameters
        ----------
        data: list[T]
            List of dataclass objects to be saved into the repository.
        """
        raise NotImplemented()


class ClickhouseRepository[T](Repository):
    """
    Repository tailored to work with Clickhouse database tables.

    Attributes
    ----------
    __tablename__: str
        Name of a table to be used in repository methods.
    """
    __tablename__: str

    def __init__(self, ch_client: ChClient):
        """
        Initialize Clickhouse Repository.

        Parameters
        ----------
        ch_client: ChClient
            aiochclient client to be used in requests of the repository.
        """
        self._ch_client = ch_client

    async def insert(self, data: list[T]):
        fields = tuple(asdict(data[0]).keys())
        values = [
            [getattr(row, field) for field in fields]
            for row in data
        ]
        await self._ch_client.execute(
            f"INSERT INTO {self.__tablename__} ({", ".join(fields)}) VALUES",
            *values
        )

