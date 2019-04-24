from airflow.models import BaseOperator

from common_plugins.calendar_plugin.hooks.create_calendar_hook import CreateCalendarHook
from common_plugins.dataframe_plugin.hooks.azure_sql_dataframe_hook import AzureSqlDataFrameHook


class CalendarToSqlTableOperator(BaseOperator):
    """

    """

    def __init__(self,
                 azure_sql_conn_id="azure_data_lake_default",
                 start=None,
                 end=None,
                 * args, **kwargs):
        super().__init__(* args, **kwargs)
        self.azure_sql_conn_id = azure_sql_conn_id
        self.start = start
        self.end = end

    def execute(self, context):
        # Create the hook
        hook = CreateCalendarHook(start=self.start, end=self.end)
        df = hook.create_dataframe()
        hook = AzureSqlDataFrameHook(sql_conn_id=self.azure_sql_conn_id)
        hook.write_dataframe(df, table_name="calendar", schema="dim")
        return
