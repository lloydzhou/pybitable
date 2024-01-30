import logging
import json
import pyparsing
import httpx
from collections import namedtuple
from urllib.parse import urlparse
from connectai.lark.sdk import Bot
from mo_sql_parsing import parse as parse_sql, format
from pep249 import ConnectionPool, Connection as ConnectionBase, Cursor as CursorBase


# pylint: disable=invalid-name
apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"

logger = logging.getLogger(__name__)
MAX_LIMIT = 20000


class ClientMixin:

    def get_columns(self, table_id):
        # TODO 这里最大支持100
        url = f'{self.host}/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/fields?page_size=100'
        return self.get(url).json()

    def create_record(self, table_id, fields):
        url = f'{self.host}/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records'
        result = self.post(url, json={'fields': fields}).json()
        record_id = result.get('data', {}).get('record', {}).get('record_id')
        if not record_id:
            raise Exception(result.get('msg', ''))
        return record_id

    def update_records(self, table_id, records):
        url = f'{self.host}/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records/batch_update'
        return self.post(url, json={'records': records}).json()

    def delete_records(self, table_id, records):
        url = f'{self.host}/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records/batch_delete'
        return self.post(url, json={'records': records}).json()

    def get_table_record(self, table_id, data, page_token='', page_size=500):
        url = f'{self.host}/open-apis/bitable/v1/apps/{self.app_token}/tables/{table_id}/records'
        return self.get(url, params=dict(page_size=page_size, page_token=page_token, **data)).json()


class PersonalBaseClient(ClientMixin):
    def __init__(self, personal_base_token='', app_token='', host='https://base-api.feishu.cn'):
        self.personal_base_token = personal_base_token
        self.app_token = app_token
        self.host = host

    def request(self, method, url, headers=None, **kwargs):
        headers = headers or dict()
        if "Authorization" not in headers:
            headers["Authorization"] = "Bearer {}".format(self.personal_base_token)
        return httpx.request(method, url, headers=headers, **kwargs)

    def get(self, url, **kwargs):
        return self.request("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self.request("POST", url, **kwargs)


class BotClient(Bot, ClientMixin):

    def __init__(self, *args, app_token='', **kwargs):
        super().__init__(*args, **kwargs)
        self.app_token = app_token


class NotSupportedError(Exception): pass
class Error(Exception): pass


class Cursor(CursorBase):
    def __init__(self, connection):
        self._connection = connection
        self.yield_per = 500

    def close(self):
        pass

    def _escape(self, v):
        value = f"{json.dumps(v, ensure_ascii=False)}"
        return value if isinstance(v, (str, int, bool)) else f"'{value}'"

    def execute(self, query, parameters=None):
        try:
            # always format json.dumps string to sql
            if isinstance(parameters, (tuple, list)):
                parameters = [self._escape(v) for v in parameters]
            elif isinstance(parameters, dict):
                parameters = {k: self._escape(v) for k, v in parameters.items()}
            else:
                parameters = ()
            parsed_query = parse_sql(query % parameters)
        except pyparsing.ParseException as e:
            raise Exception(query)

        logger.debug("execute %r", parsed_query)
        if 'select' in parsed_query and 'from' in parsed_query:
            return self.do_select(parsed_query)
        elif 'insert' in parsed_query:
            return self.do_insert(parsed_query)
        elif 'update' in parsed_query:
            return self.do_update(parsed_query)
        elif 'delete' in parsed_query:
            return self.do_delete(parsed_query)

        return self

    def executemany(self, operation, seq_of_parameters):
        for parameters in seq_of_parameters:
            logger.debug(f'executes with parameters {parameters}.')
            self.execute(operation, parameters)

    def _query_all(self, table_id, data):
        page_token, page_size = '', self.yield_per
        while True:
            result = self._connection.bot.get_table_record(table_id, data, page_token=page_token, page_size=page_size)
            logger.debug("result %r %r --> %r", table_id, data, result)
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
        Row = namedtuple('Row', alias, rename=True)
        values = []
        for name in names:
            if name in item:
                values.append(item[name])
            elif name in item['fields']:
                value = item['fields'][name]
                # 多行文本是一个数组
                if isinstance(value, list) and len(value) > 0 and 'text' in value[0]:
                    value = ''.join([l['text'] for l in value])
                values.append(value)
            else:
                values.append(None)  # TODO

        return Row(*values)

    @property
    def description(self):
        columns = self._columns[0] if hasattr(self, '_columns') else ['record_id']
        # TODO
        return [(name, 'varchar', None, None, None, None, True) for name in columns]

    def get_columns(self, parsed):
        if isinstance(parsed['select'], list):
            return [i['value'] for i in parsed['select']], [i.get('name', i['value']) for i in parsed['select']]
        elif isinstance(parsed['select'], dict):
            if 'value' in parsed['select']:
                value = parsed['select']['value']
                return [value], [parsed['select'].get('name', value)]
            elif 'all_columns' in parsed['select']:
                result = self._connection.bot.get_columns(parsed['from'])
                items = result.get('data', {}).get('items', [])
                return [i['field_name'] for i in items], [i['field_name'] for i in items]
        return [], []

    def _process_filter1(self, where):
        if not('or' in where or 'and' in where):
            where = {'and': where if isinstance(where, list) else [where]}

        conjunction = 'or' if 'or' in where else 'and'
        conditions = [c for c in where.get(conjunction, []) if bool(c)]
        filters = []
        if len(conditions) > 0:
            filters.append(conjunction.upper())
            filters.append('(')
            for c, i in enumerate(conditions):
                comma = ',' if c > 0 else ''
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
                if 'eq' in i:
                    field_name, value = i['eq']
                    if isinstance(value, dict) and 'literal' in value:
                        if '"' == value["literal"][0]:
                            filters.append(f'{comma}CurrentValue.[{field_name}]={value["literal"]}')
                        else:
                            filters.append(f'{comma}CurrentValue.[{field_name}]="{value["literal"]}"')
                    else:
                        filters.append(f'{comma}CurrentValue.[{field_name}]={json.dumps(value, ensure_ascii=False)}')
                elif 'neq' in i:
                    field_name, value = i['neq']
                    if isinstance(value, dict) and 'literal' in value:
                        filters.append(f'{comma}NOT(CurrentValue.[{field_name}]="{value["literal"]}")')
                    else:
                        filters.append(f'{comma}NOT(CurrentValue.[{field_name}]={json.dumps(value["literal"], ensure_ascii=False)})')
                elif 'lt' in i:
                    field_name, value = i['lt']
                    # 只支持可以比较大小的
                    filters.append(f'{comma}CurrentValue.[{field_name}]<{value["literal"]}')
                elif 'lte' in i:
                    field_name, value = i['lte']
                    filters.append(f'{comma}CurrentValue.[{field_name}]<={value["literal"]}')
                elif 'gt' in i:
                    field_name, value = i['gt']
                    filters.append(f'{comma}CurrentValue.[{field_name}]>{value["literal"]}')
                elif 'gte' in i:
                    field_name, value = i['gte']
                    filters.append(f'{comma}CurrentValue.[{field_name}]>={value["literal"]}')
                elif 'like' in i:
                    field_name, value = i['like']
                    # 这里一定是字符串
                    if '"' == value["literal"][0]:
                        filters.append(f'{comma}CurrentValue.[{field_name}].contains({value["literal"]})')
                    else:
                        filters.append(f'{comma}CurrentValue.[{field_name}].contains("{value["literal"]}")')
                elif 'in' in i:
                    field_name, value = i['in']
                    is_literal = isinstance(value, dict) and 'literal' in value
                    value = value['literal'] if is_literal else value
                    if isinstance(value, list):
                        t = ['OR(']
                        for l in value:
                            if is_literal:
                                t.append(f'CurrentValue.[{field_name}]="{l}"')
                            else:
                                t.append(f'CurrentValue.[{field_name}]={l}')
                        t.append(')')
                        filters.append(comma + ''.join(t))
                elif 'missing' in i:
                    field_name = i['missing']
                    filters.append(f'{comma}CurrentValue.[{field_name}]=""')
                elif 'exists' in i:
                    field_name = i['exists']
                    filters.append(f'{comma}NOT(CurrentValue.[{field_name}]="")')
            filters.append(')')
        return ''.join(filters)

    def do_select(self, parsed):
        table_id = parsed['from']
        self._offset = int(parsed['offset'].get('literal', 0) if isinstance(parsed.get('offset'), dict) else parsed.get('offset', 0))
        self._limit = int(parsed['limit'].get('literal', MAX_LIMIT) if isinstance(parsed.get('limit'), dict) else parsed.get('limit', MAX_LIMIT))
        self._columns = self.get_columns(parsed)

        orderby = parsed.get('orderby', [])
        if isinstance(orderby, dict):
            orderby = [orderby]
        sort = []
        for i in orderby:
            field_name = i['value']
            if field_name in self._columns[1]:
                field_name = self._columns[0][self._columns[1].index(field_name)]
            sort.append(f"{field_name} {i.get('sort', '')}")

        filter_str = self._process_filter1(parsed.get('where', {}))
        self._result_set = self._query_all(table_id, {
            'field_names': json.dumps([i for i in self._columns[0] if i not in ['record_id']], ensure_ascii=False),  # record_id
            'sort': json.dumps(sort, ensure_ascii=False),
            'filter': filter_str,
            'automatic_fields': True,
        })
        return self

    def do_insert(self, parsed):
        fields = {}
        for index, column in enumerate(parsed['columns']):
            value = parsed['query']['select'][index]['value']
            if isinstance(value, dict) and 'literal' in value:
                try:
                    fields[column] = json.loads(value['literal'])
                except Exception as e:
                    logger.debug(e)
                    fields[column] = value['literal']
            else:
                fields[column] = value

        self.lastrowid = self._connection.bot.create_record(parsed['insert'], fields)
        return self.lastrowid

    def _get_literal_value(self, value):
        try:
            return json.loads(value)
        except:
            return value

    def _get_record_id_by_where(self, where, table_id):
        if 'eq' in where and 'record_id' == where['eq'][0]:
            return [self._get_literal_value(where['eq'][1])]

        sql = format({ 'from': table_id, 'where': where, 'select': [{ 'value': 'record_id' }] })
        cursor = Cursor(self._connection)
        cursor.execute(sql)
        records = cursor.fetchall()
        return [record.record_id for record in records]

    def do_update(self, parsed):
        fields = {field_name: self._get_literal_value(value['literal']) if isinstance(value, dict) and 'literal' in value else value for field_name, value in parsed['set'].items()}
        table_id = parsed['update']
        record_ids = self._get_record_id_by_where(parsed.get('where', {}), table_id)
        records = [{'record_id': record_id, 'fields': fields} for record_id in record_ids]
        logger.debug('update %r %r %r', record_ids, fields, records)
        self.rowcount = len(record_ids)
        if self.rowcount > 0:
            self._connection.bot.update_records(table_id, records)

    def do_delete(self, parsed):
        table_id = parsed['delete']
        record_ids = self._get_record_id_by_where(parsed.get('where', {}), table_id)
        logger.debug('delete %r %r', table_id, record_ids)
        self.rowcount = len(record_ids)
        if self.rowcount > 0:
            self._connection.bot.delete_records(table_id, record_ids)

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
    # bitable+pybitable://<personal_base_token>@open.feishu.cn/<app_token>
    def __init__(self, connect_string, **kwargs):
        if connect_string:
            result = urlparse(connect_string)
            self.app_id = result.username
            self.app_secret = result.password
            self.host = result.hostname
            # self.scheme = result.scheme
            self.app_token = result.path[1:]
        elif 'host' in kwargs:
            self.app_id = kwargs.get('username', '')
            self.app_secret = kwargs.get('password', '')
            self.host = kwargs.get('host', '')
            self.app_token = kwargs.get('database', '')
        if self.app_id and self.app_secret:
            self.bot = BotClient(
                app_id=self.app_id,
                app_secret=self.app_secret,
                app_token=self.app_token,
                host=f"https://{self.host}",
            )
        else:
            # 使用个人授权码可以直接调用
            self.bot = PersonalBaseClient(
                personal_base_token=self.app_id or self.app_secret,
                app_token=self.app_token,
                host=f"https://{self.host}",
            )

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return Cursor(self)


def connect(connection_string: str = "", **kwargs) -> Connection:
    """Connect to a Lark BITable, returning a connection."""
    return Connection(connection_string, **kwargs)

