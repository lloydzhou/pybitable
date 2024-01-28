import logging
import pyparsing
from collections import OrderedDict
from mo_sql_parsing import parse as parse_sql
from urllib.parse import urlparse
from connectai.lark.sdk import Bot
from mo_sql_parsing import parse as parse_sql, format
from pep249 import ConnectionPool, Connection as ConnectionBase, Cursor as CursorBase


# pylint: disable=invalid-name
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"

string_types = str
logger = logging.getLogger(__name__)


class NotSupportedError(Exception): pass



class Cursor(CursorBase):
    def __init__(self, connection):
        self._connection = connection
        self.yield_per = 20

    def close(self):
        pass

    def execute(self, query, parameters=()):
        logging.error("execute %r", query)
        try:
            parsed_query = parse_sql(query)
        except pyparsing.ParseException as e:
            raise Exception(query)

        logging.error("execute %r", parsed_query)
        # {'select': {'all_columns': {}}, 'from': 'tbl2w2QJgo6YCthm', 'limit': 1, 'offset': 10}
        if 'select' in parsed_query and 'from' in parsed_query:
            return self.do_select(parsed_query)

        return None

    def executemany(self, operation, seq_of_parameters):
        for parameters in seq_of_parameters:
            logger.debug(f'executes with parameters {parameters}.')
            self.execute(operation, parameters)

    def _query_all(self, table_id, data):
        page_token, page_size = '', self.yield_per
        while True:
            url = f'{self._connection.bot.host}/open-apis/bitable/v1/apps/{self._connection.app_token}/tables/{table_id}/records/search?page_size={page_token}&page_token={page_token}'
            result = self._connection.bot.post(url, json=data).json()
            for item in result.get('data', {}).get('items', []):
                if self._offset > 0:
                    self._offset = self._offset - 1
                    continue
                elif self._limit > 0:
                    # yield item
                    yield item['fields']
                    self._limit = self._limit - 1
                else:
                    break

            if result.get('has_more'):
                page_token = result['page_token']
            else:
                break

    def do_select(self, parsed):
        table_id = parsed['from']
        self._offset = parsed.get('offset', 0)
        self._limit = parsed.get('limit', 20000)  # TODO

        self._result_set = self._query_all(table_id, {
            'field_names': None,
            'sort': [],
            'filter': {
                'conjunction': 'and',  # or
                'conditions': [],  # {field_name, operator, value}
            },
            'automatic_fields': True,
        })
        return self

    def fetchone(self):
        try:
            return self.__next__()
        except Exception as e:
            return None

    def fetchall(self):
        return list(self)

    def fetchmany(self, size):
        self._limit = size
        return self.fetchall()

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._result_set)


class Connection(ConnectionBase):
    # bitable+pybitable://<app_id>:<app_secret>@open.feishu.cn/<app_token>
    def __init__(self, connect_string, **kwargs):
        result = urlparse(connect_string)
        self.app_id = result.username
        self.app_secret = result.password
        self.host = result.hostname
        # self.scheme = result.scheme
        self.app_token = result.path[1:]
        self.bot = Bot(
            app_id=self.app_id,
            app_secret=self.app_secret,
            host=f"https://{self.host}",
        )

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return Cursor(self)


