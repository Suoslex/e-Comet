CREATE TABLE IF NOT EXISTS repositories_positions
(
    date     date,
    repo     String,
    position UInt32
) ENGINE = ReplacingMergeTree
      ORDER BY (date, repo);