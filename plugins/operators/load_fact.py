from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = "#F98866"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 sql_query="",
                 table="",
                 append_mode="",
                 *args, **kwargs):
        #  map our class properties to the constructor parameters
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.sql_query = sql_query
        self.table = table,

    def execute(self, context):
        # create a connection to redshift
        self.log.info("Loading data into table")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # convert the tuples to strings for inclusion
        tablename = ''.join(self.table)
        query = ''.join(self.sql_query)
        # Once we've truncated if necessary we inlcude the parameters into the query SQL
        self.sql_query = "INSERT INTO {0} {1}".format(
            tablename, query)
        self.log.info("SQL Query: {}".format(self.sql_query))
        redshift.run(self.sql_query)
