from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 test_result="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query,
        self.table = table,
        self.test_result = test_result

    def execute(self, context):
        self.log.info('Checking data quality')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        query = ''.join(self.sql_query)
        self.log.info("Data quality query:{}".format(self.sql_query))
        # get the first field of the first record returned which contains the count
        query_result = redshift.get_records(query)[0][0]
        self.log.info("Data quality query result:{}".format(query_result))
        self.log.info(
            "Data quality expected result:{}".format(self.test_result))
        if query_result != self.test_result:
            raise Exception("Data quality test failed")
