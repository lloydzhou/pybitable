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

## using sqlalchemy

```

from sqlalchemy import create_engine, Column, String, Text, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine(db_url, echo=False)
Session = sessionmaker(bind=engine)
session = Session()

with engine.connect() as conn:
    result = conn.execute(
        text("select `文本` as a from tblID0QbOnjktwdC")
    )
    for row in result:
        print(f"{row}")

Base = declarative_base()

class BITable1(Base):
    __tablename__ = 'tblID0QbOnjktwdC'
    record_id = Column(String(32), primary_key=True)
    文本 = Column(Text, nullable=True, server_default=text("''"), comment="文本")
    单选 = Column(String(32), nullable=True, server_default=text("''"), comment="单选")
    多选 = Column(Text, nullable=True, server_default=text("''"), comment="多选")

Session = sessionmaker(bind=engine)
session = Session()

for item in session.query(BITable1).all():
    print('record_id: ', item.record_id, '文本: ', item.文本, '单选', item.单选, '多选', item.多选)

print('engine', engine, BITable1)
```

![image](https://github.com/lloydzhou/pybitable/assets/1826685/c12009c4-0ea0-4a30-babb-97a6604142e7)

![image](https://github.com/lloydzhou/pybitable/assets/1826685/fa85ed0b-2474-4caf-a4ef-5185afceebce)

