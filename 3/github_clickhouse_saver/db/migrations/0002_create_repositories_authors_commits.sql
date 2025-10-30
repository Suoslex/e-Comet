CREATE TABLE IF NOT EXISTS repositories_authors_commits
(
    date        date,
    repo        String,
    author      String,
    commits_num Int32
) ENGINE = ReplacingMergeTree
      ORDER BY (date, repo, author);
