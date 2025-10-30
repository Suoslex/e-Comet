import asyncio
import argparse

from github_clickhouse_saver.db.client import create_clickhouse_client
from github_clickhouse_saver.saver import GithubClickhouseSaver
from github_clickhouse_saver.settings import settings


async def main(
        limit: int,
        batch_size: int,
        scrapper_page_size: int,
        scrapper_max_concurrent_requests: int,
        scrapper_requests_per_second: int
):
    ch_client = create_clickhouse_client(
        url=settings.clickhouse_url,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database
    )
    async with GithubClickhouseSaver(ch_client=ch_client) as github_saver:
        await github_saver.save_top_repos(
            access_token=settings.github_access_token,
            limit=limit,
            batch_size=batch_size,
            scrapper_page_size=scrapper_page_size,
            scrapper_max_concurrent_requests=scrapper_max_concurrent_requests,
            scrapper_requests_per_second=scrapper_requests_per_second
        )


def parse_cmd_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Github Clickhouse Saver")
    parser.add_argument(
        "-l",
        "--limit",
        type=int,
        default=10,
        help="Maximum number of fetched top Github repositories"
    )
    parser.add_argument(
        "-b",
        "--batch-size",
        type=int,
        default=20,
        help="Maximum number of repositories saved to clickhouse at once."
    )
    parser.add_argument(
        "-sps",
        "--scrapper-page-size",
        type=int,
        default=20,
        help="Maximum number of repositories fetched at once from Github API."
    )
    parser.add_argument(
        "-smcq",
        "--scrapper-max-concurrent-requests",
        type=int,
        default=20,
        help="Maximum number of concurrent requests made to Github API."
    )
    parser.add_argument(
        "-srps",
        "--scrapper-requests-per-second",
        type=int,
        default=20,
        help="Maximum number of requests made per second to Github API."
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    cmd_args = parse_cmd_args()
    asyncio.run(
        main(
            limit=cmd_args.limit,
            batch_size=cmd_args.batch_size,
            scrapper_page_size=cmd_args.scrapper_page_size,
            scrapper_max_concurrent_requests=(
                cmd_args.scrapper_max_concurrent_requests
            ),
            scrapper_requests_per_second=cmd_args.scrapper_requests_per_second
        )
    )
