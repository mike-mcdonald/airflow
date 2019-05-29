
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class MsSqlOperator(BaseOperator):
    """
    Executes sql code in a specific MySQL database
    :param sql: the sql code to be executed. Can receive a str representing a
        sql statement, a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
        (templated)
    :type sql: str or list[str]
    :param mssql_conn_id: reference to a specific mysql database
    :type mssql_conn_id: str
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: mapping or iterable
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    :param database: name of database which overwrite defined one in connection
    :type database: str
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql, mssql_conn_id='mssql_default', parameters=None,
            autocommit=True, database=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id,
                         schema=self.database)
        hook.run(
            self.sql,
            autocommit=self.autocommit,
            parameters=self.parameters)
