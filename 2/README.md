# GithubReposScrapper

Класс для работы с Github API, позволяющий получить топ Github репозиториев
на данный момент.

## Установка и запуск

Самым простым способом будет установка напрямую из git:

```bash
python -m pip install "git+https://github.com/Suoslex/e-Comet.git@main#subdirectory=2/"
```

Вы так же можете установить все необходимое вручную.

Для этого нужно установить все необходимые зависимости с помощью pip:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

либо с помощью uv:

```bash
uv venv
source .venv/bin/activate
uv sync
```

Для использования скрипта в своих проектах, используйте класс 
`github_repos_scrapper.scrapper.GithubReposScrapper`, который требует
на вход рабочий Github API `access_token`.

## Пример использования

```python
import asyncio

from github_repos_scrapper.scrapper import GithubReposScrapper

access_token = "github_token..."

async def main():
    async with GithubReposScrapper(
            access_token=access_token,
            max_concurrent_requests=30,
            requests_per_second=30
    ) as scrapper:
        result = await scrapper.get_repositories(20)
        print(result)

if __name__ == "__main__":
    asyncio.run(main())
```


## Важные детали реализации

### 1. Асинхронное получение данных с помощью `async_execute`

`github_clickhouse_saver.utils.async_execute(func, args_list, max_workers)`

Главная функция, отвечающая за распараллеливание работы в GithubReposScrapper.
Ее главная особенность в том, что она создает определенное количество
тасок (workers), которые выполняют переданную ей функцию `func` с переданными
аргументами `args_list`. От количества "воркеров" зависит скорость работы
и количество используемой оперативной памяти, поэтому важно найти баланс.

### 2. Ограничение максимального количества одновременных запросов (MCR)

Реализовано с помощью `asyncio.Semaphore`, встроенный в метод отправки
запросов `GithubReposScrapper.__make_request`. Любой запрос к Github API
происходит через этот метод, поэтому можно быть уверенным, что одновременно
этим методом смогут пользоваться только определенное количество тасок.

### 3. Ограничение количества запросов в секунду (RPS)

Реализовано с помощью устанавливаемой библиотеки `aiolimiter`, которая
проверена временем и имеет большое количество звезд на Github.
При желании, можно было реализовать ограничение RPS без лишних зависимостей
с помощью Token Bucket, но проверенное готовое решение всегда лучше.
