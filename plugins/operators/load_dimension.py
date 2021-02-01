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

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query,
        self.append_mode = append_mode

    def execute(self, context):
        self.log.info('LoadDimensionOperator on {}'.format(self.table))
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        tablename = ''.join(self.table)
        query = ''.join(self.sql_query)
        # check whether we should append or truncate the table first.
        if self.append_mode != True:
            self.sql_query = "TRUNCATE TABLE {}".format(
                self.table)
            redshift.run(self.sql_query)
        self.sql_query = "INSERT INTO {0} {1}".format(tablename, query)
        self.log.info("SQL Query:"+self.sql_query)
        redshift.run(self.sql_query)
