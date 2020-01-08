
To get this working you will first need to configure where binaries are on your system. Edit `SublimeQueryDB.sublime-settings`

```json
{
    "query_db_executables": {
        "psql": "/usr/bin/psql",
        "bq"  : "/home/mbarkhau/Downloads/google-cloud-sdk/bin/bq",
    },
}
```

To perform a query, open a new tab and write something like this:

```sql
-- db=bigquery://<auth_name>@<project>/<dataset>

SELECT * FROM INFORMATION_SCHEMA.TABLES
```

Highlight everything and press `alt+shift+r`.
