"""BITable CLI

Usage:
  pybitable [options] <app_token>
  pybitable --help
  pybitable --version

Options:
  --help                                 Show this screen.
  --version                              Show version.
  -h <host>, --host <host>               host.
  -p <password>, --password <password>   password.
  -u <username>, --username <username>   username.

"""  # noqa: E501

from __future__ import unicode_literals

import os

from docopt import docopt
from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles.pygments import style_from_pygments_cls
from pygments.lexers import SqlLexer
from pygments.styles import get_style_by_name
from tabulate import tabulate

from pybitable import connect, __version__


keywords = [
    'and',
    'asc',
    'by',
    'date',
    'datetime',
    'desc',
    'false',
    'format',
    'group',
    'label',
    'limit',
    'not',
    'offset',
    'options',
    'or',
    'order',
    'select',
    'true',
    'where',
    'show',
    'tables',
    'views',
]

aggregate_functions = [
]

scalar_functions = [
    'year',
    'month',
    'day',
    'hour',
    'minute',
    'second',
    'millisecond',
    'quarter',
    'upper',
    'lower',
]


def main():
    history = FileHistory(os.path.expanduser('~/.pybitable_history'))

    arguments = docopt(__doc__, version=__version__)

    app_token = arguments['<app_token>']
    if 'bitable://' not in app_token:
        password = arguments["--password"]
        assert password
        username = arguments["--username"] or ''
        host = arguments["--host"] or 'open.feishu.cn'
        url = f'bitable+pybitable://{username}:{password}@{host}/{app_token}'
    else:
        url = app_token

    connection = connect(url)
    cursor = connection.cursor()

    lexer = PygmentsLexer(SqlLexer)
    words = keywords + aggregate_functions + scalar_functions
    try:
        cursor.execute('show tables')
        tables = cursor.fetchall()
        words = words + [i[0] for i in tables]
    except Exception as e:
        print(e)
    completer = WordCompleter(words, ignore_case=True)
    style = style_from_pygments_cls(get_style_by_name('manni'))

    while True:
        try:
            query = prompt(
                'sql> ',
                lexer=lexer,
                completer=completer,
                style=style,
                history=history
            )
        except (EOFError, KeyboardInterrupt):
            break  # Control-D pressed.

        # run query
        query = query.strip('; ').replace('%', '%%')
        if query:
            try:
                cursor.execute(query)
                result = cursor.fetchall()
            except Exception as e:
                print(e)
                continue

            columns = [t[0] for t in cursor.description or []]
            print(tabulate(result, headers=columns))

    print('bye!')

if __name__ == "__main__":
    main()

