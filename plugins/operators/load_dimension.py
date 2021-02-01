from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 sql_query="",
                 redshift_conn_id="",
                 append_mode="",
                 * args, **kwargs):
        # map our constructor properties to parameters
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query,
        self.append_mode = append_mode

    def execute(self, context):
        self.log.info('LoadDimensionOperator on {}'.format(self.table))
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # string conversion
        table_str = ''.join(self.table)
        query_str = ''.join(self.sql_query)
        # check whether we should append or truncate the table first.
        if self.append_mode != True:
            self.sql_query = "TRUNCATE TABLE {}".format(
                self.table)
            redshift.run(self.sql_query)
        # once truncated we can run the dimension load process
        # first interpolate strings
        self.sql_query = "INSERT INTO {0} {1}".format(table_str, query_str)
        self.log.info("SQL Query:{}".format(self.sql_query))
        # now execute the SQL
        redshift.run(self.sql_query)
