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
                 db_name="",
                 table="",
                 append_mode="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.sql_query = sql_query,
        self.table = table,
        self.append_mode = append_mode

    def execute(self, context):
        self.log.info("Loading data into {}").format(
            self.table)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # check whether we shoudl append or truncate the table first.
        if self.append_mode != True:
            sql_statement = "TRUNCATE TABLE {}".format(
                self.table)
            redshift.run(sql_statement)
        sql_statement = "INSERT INTO {} {}".format(
            self.table, self.sql_statement)
        redshift.run(sql_statement)
