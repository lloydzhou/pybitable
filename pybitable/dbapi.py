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


class ObjectDict(dict):

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        del self[name]


class Cursor(CursorBase):
    def __init__(self, connection):
        self._connection = connection
        self.yield_per = 20

    def close(self):
        pass

    def execute(self, query, parameters=()):
        logger.debug("execute %r", query)
        try:
            parsed_query = parse_sql(query)
        except pyparsing.ParseException as e:
            raise Exception(query)

        logger.debug("execute %r", parsed_query)
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
            # logger.error("result %r --> %r", data, result)
            if 'error' in result:
                raise Exception(result['error'].get('message', result.get('msg')))
            for item in result.get('data', {}).get('items', []):
                if self._offset > 0:
                    self._offset = self._offset - 1
                    continue
                elif self._limit > 0:
                    # yield item
                    # yield item['fields']
                    # 按照self._columns的结构返回数据
                    names, alias = self._columns
                    yield self._process_result(item, names, alias)
                    self._limit = self._limit - 1
                else:
                    break

            if result.get('has_more'):
                page_token = result['page_token']
            else:
                break

    def _process_result(self, item, names, alias):
        values = []
        for name in names:
            if name in item:
                values.append(item[name])
            elif name in item['fields']:
                value = item['fields'][name]
                values.append(value)
            else:
                values.append(None)  # TODO

        return ObjectDict(zip(alias, values))

    def get_columns(self, parsed):
        if isinstance(parsed['select'], list):
            return [i['value'] for i in parsed['select']], [i.get('name', i['value']) for i in parsed['select']]
        elif isinstance(parsed['select'], dict):
            if 'value' in parsed['select']:
                value = parsed['select']['value']
                return [value], [parsed['select'].get('name', value)]
            elif 'all_columns' in parsed['select']:
                # TODO 这里通过接口获取所有字段
                return [], []
        return [], []

    def _process_filter(self, where):
        print('_process_filter', where)
        if not('or' in where or 'and' in where):
            where = {'and': where if isinstance(where, list) else [where]}

        conjunction = 'or' if 'or' in where else 'and'
        conditions = []
        for i in where.get(conjunction, []):
            """
            operator 可选值有：
            is：等于
            isNot：不等于
            contains：包含
            doesNotContain：不包含
            isEmpty：为空
            isNotEmpty：不为空
            isGreater：大于
            isGreaterEqual：大于等于
            isLess：小于
            isLessEqual：小于等于
            like：like
            in：in
            """
            field_name, operator, value = '', '', ''
            if 'eq' in i:
                operator = 'is'
                field_name, value = i['eq']
            elif 'neq' in i:
                operator = 'isNot'
                field_name, value = i['neq']
            elif 'lt' in i:
                operator = 'isLess'
                field_name, value = i['lt']
            elif 'lte' in i:
                operator = 'isLessEqual'
                field_name, value = i['lte']
            elif 'gt' in i:
                operator = 'isGreater'
                field_name, value = i['gt']
            elif 'gte' in i:
                operator = 'isGreaterEqual'
                field_name, value = i['gte']
            elif 'like' in i:
                operator = 'like'
                field_name, value = i['like']
            elif 'in' in i:
                print('in', i)
                operator = 'in'
                field_name, value = i['in']
            elif 'missing' in i:
                operator = 'isEmpty'
                field_name, value = i['missing'], ''
            elif 'exists' in i:
                operator = 'isNotEmpty'
                field_name, value = i['exists'], ''
            # TODO filter.children

            if operator:
                if field_name in self._columns[1]:
                    field_name = self._columns[0][self._columns[1].index(field_name)]
                conditions.append({
                    'field_name': field_name,
                    'operator': operator,
                    'value': value if isinstance(value, list) else [value],
                })
        return conjunction, conditions

    def do_select(self, parsed):
        table_id = parsed['from']
        self._offset = parsed.get('offset', 0)
        self._limit = parsed.get('limit', 20000)  # TODO
        self._columns = self.get_columns(parsed)

        orderby = parsed.get('orderby', [])
        if isinstance(orderby, dict):
            orderby = [orderby]
        sort = []
        for i in orderby:
            field_name = i['value']
            if field_name in self._columns[1]:
                field_name = self._columns[0][self._columns[1].index(field_name)]
            sort.append({
                'field_name': field_name,
                'desc': i.get('sort', '').lower() == 'desc'
            })

        conjunction, conditions = self._process_filter(parsed.get('where', {}))
        self._result_set = self._query_all(table_id, {
            'field_names': [i for i in self._columns[0] if i not in ['record_id']],  # record_id
            'sort': sort,
            'filter': {
                'conjunction': conjunction,
                'conditions': conditions,
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


