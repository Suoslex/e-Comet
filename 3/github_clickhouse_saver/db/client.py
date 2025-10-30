from aiochclient import ChClient

def create_clickhouse_client(
        url: str,
        user: str,
        password: str,
        database: str,
) -> ChClient:
    """
    Creates client object to be used to work with Clickhouse.

    Parameters
    ----------
    url: str
        URL of the Clickhouse database.
    user: str
        User to be used when accessing Clickhouse.
    password: str
        Password to authenticate the user.
    database: str
        Clickhouse database to use in the client.

    Returns
    -------
    ChClient
        aiochclient client.
    """
    return ChClient(url=url, user=user, password=password, database=database)