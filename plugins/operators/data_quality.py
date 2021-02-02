from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 field="",
                 expected_result="",
                 *args, **kwargs):
        #  map our class properties to the constructor parameters
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query,
        self.table = table,
        self.field = field,
        self.expected_result = expected_result

    def execute(self, context):
        self.log.info("Checking data quality")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # convert properties to strings
        query_str = "".join(self.sql_query)
        table_str = "".join(self.table)
        field_str = "".join(self.field)
        # inerpolate parameters into query
        formatted_sql = query_str.format(
            table_str,
            field_str
        )
        self.log.info("Data quality query:{}".format(formatted_sql))
        # get the first field of the first field and first record returned which contains the null count
        query_result = redshift.get_records(formatted_sql)[0][0]
        self.log.info("Data quality query result:{}".format(query_result))
        self.log.info(
            "Data quality expected result:{}".format(self.expected_result))
        # test whether we get the expected result - if we don"t raise an exception
        if query_result != self.expected_result:
            raise Exception("Data quality test failed")
