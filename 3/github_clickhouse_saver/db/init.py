from pathlib import Path

from aiochclient import ChClient

from github_clickhouse_saver.logger import logger


async def init_clickhouse(ch_client: ChClient):
    """
    Initializes clickhouse database to be ready for the saver.
    Please take a note that all current migrations can be run several times
    without harm, so the script is pretty simple (executes each of SQLs).
    Some deeper work needs to be done if more complex migration comes.

    Parameters
    ----------
    ch_client: ChClient
        aiochclient client object to be used in initialization requests.
    """
    logger.debug("Initializing clickhouse")
    migration_files = Path(__file__).parent / 'migrations'
    for migration in migration_files.glob("*.sql"):
        logger.debug(f"Found migration sql {migration}. Applying")
        with open(migration) as file:
            migration_sql = file.read()
        await ch_client.execute(migration_sql)
