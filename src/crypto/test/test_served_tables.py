from crypto.elt.transform import Transform
from database.postgres import PostgresDB

class TestServedTables:

    """ Test the transformations on the serving tables """

    def test_transform_serving_coins_history_with_Tranform_method(self):

        """ Simply run the sql code in the served_coins_history.sql script """

        table_name = "served_coins_history" # same as file_name
        source_engine = PostgresDB.create_pg_engine("serving")
        path = "crypto/models/transform"

        # map served tables to serving tables 
        table_mapping = {
            'served_coins_history': 'serving_coin_history'
        }


        transform_node = Transform(
            model=table_name,
            engine=source_engine,
            models_path=path,
            table_mapping=table_mapping
        )

        assert transform_node.run() == True
