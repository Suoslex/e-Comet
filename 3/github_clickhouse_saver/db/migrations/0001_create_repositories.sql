CREATE TABLE IF NOT EXISTS repositories
(
    name     String,
    owner    String,
    stars    Int32,
    watchers Int32,
    forks    Int32,
    language String,
    updated  datetime
) ENGINE = ReplacingMergeTree(updated)
      ORDER BY name;