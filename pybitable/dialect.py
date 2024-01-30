from sqlalchemy import exc, pool, types, inspect
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler

_type_map = {
    'bit': types.BOOLEAN,
    'bigint': types.BIGINT,
    'binary': types.LargeBinary,
    'boolean': types.BOOLEAN,
    'date': types.DATE,
    'decimal': types.DECIMAL,
    'double': types.FLOAT,
    'int': types.INTEGER,
    'integer': types.INTEGER,
    'interval': types.Interval,
    'smallint': types.SMALLINT,
    'timestamp': types.TIMESTAMP,
    'time': types.TIME,
    'varchar': types.String,
    'any': types.String,
}


class BITableCompiler(compiler.SQLCompiler):

    def visit_column(self, column, **kwargs):
        if column.table is not None:
            column.table.named_with_column = False
        return super(BITableCompiler, self).visit_column(column, **kwargs)

    def visit_table(
        self,
        table,
        asfrom=False,
        iscrud=False,
        ashint=False,
        fromhints=None,
        use_schema=False,
        **kwargs
    ):
        return super(BITableCompiler, self).visit_table(
            table, asfrom, iscrud, ashint, fromhints, False, **kwargs
        )


class BITableTypeCompiler(compiler.GenericTypeCompiler): pass


class BITableIdentifierPreparer(compiler.IdentifierPreparer): pass


class BITableDialect(default.DefaultDialect):
    # pylint: disable=abstract-method

    name = "bitable"
    driver = "rest"
    preparer = BITableIdentifierPreparer
    statement_compiler = BITableCompiler
    type_compiler = BITableTypeCompiler
    poolclass = pool.SingletonThreadPool
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True
    supports_statement_cache = True
    postfetch_lastrowid = True

    def __init__(self, **kw):
        default.DefaultDialect.__init__(self, **kw)
        self.supported_extensions = []

    @classmethod
    def dbapi(cls):
        from . import dbapi as module

        return module

    def do_rollback(self, dbapi_connection):
        # No transactions for BITable
        pass

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """BITable has no support for foreign keys.  Returns an empty list."""
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """BITable has no support for indexes.  Returns an empty list. """
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """BITable has no support for primary keys.  Retunrs an empty list."""
        return []

