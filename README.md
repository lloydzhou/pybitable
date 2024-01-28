# pybitable

```
from pybitable import Connection, ConnectionPool

db_url = 'bitable+pybitable://<app_id>:<app_secret>@open.feishu.cn/<app_token>'

conn_pool = ConnectionPool(
    maxsize=10,
    connection_factory=lambda: Connection(db_url),
)

with conn_pool.connect() as connection:
    print('connect', connection)
    cursor = connection.cursor()
    result = cursor.execute('select * from tbl2w2QJgo6YCthm')
    cursor.close()
```


