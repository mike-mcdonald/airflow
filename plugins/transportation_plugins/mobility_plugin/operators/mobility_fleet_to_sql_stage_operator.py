from common_plugins.mssql_plugin.operators.mssql_operator import MsSqlOperator


class MobilityFleetToSqlStageOperator(MsSqlOperator):
    """

    """

    def __init__(self,
                 * args, **kwargs):
        sql = """
        INSERT INTO etl.stage_fleet_count (
            date_key
            ,provider_key
            ,time
            ,available
            ,reserved
            ,unavailable
            ,removed
            ,batch
        )
        SELECT
        date_key
        ,provider_key
        ,time
        ,a.count
        ,r.count
        ,u.count
        ,x.count
        ,batch
        FROM etl.extract_fleet_count AS e
        OUTER APPLY (
            SELECT COUNT(start_hash) AS count
            FROM fact.state AS f
            WHERE f.date_key = e.date_key
            AND f.provider_key = e.provider_key
            AND f.start_time <= e.time
            AND COALESCE(f.end_time, cast('12/31/9999 23:59:59.9999' as datetime2)) >= e.time
            AND state = 'available'
        ) AS a
        OUTER APPLY (
            SELECT COUNT(start_hash) AS count
            FROM fact.state AS f
            WHERE f.date_key = e.date_key
            AND f.provider_key = e.provider_key
            AND f.start_time <= e.time
            AND COALESCE(f.end_time, cast('12/31/9999 23:59:59.9999' as datetime2)) >= e.time
            AND state = 'reserved'
        ) AS r
        OUTER APPLY (
            SELECT COUNT(start_hash) AS count
            FROM fact.state AS f
            WHERE f.date_key = e.date_key
            AND f.provider_key = e.provider_key
            AND f.start_time <= e.time
            AND COALESCE(f.end_time, cast('12/31/9999 23:59:59.9999' as datetime2)) >= e.time
            AND state = 'unavailable'
        ) AS u
        OUTER APPLY (
            SELECT COUNT(start_hash) AS count
            FROM fact.state AS f
            WHERE f.date_key = e.date_key
            AND f.provider_key = e.provider_key
            AND f.start_time <= e.time
            AND COALESCE(f.end_time, cast('12/31/9999 23:59:59.9999' as datetime2)) >= e.time
            AND state = 'removed'
        ) AS x
        WHERE e.batch = '{{ ts_nodash }}'
        """
        super().__init__(sql, *args, **kwargs)
