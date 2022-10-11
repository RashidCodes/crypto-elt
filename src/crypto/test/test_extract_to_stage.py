from crypto.elt.extract import Extract
from crypto.pipeline.extract_load_pipeline import ExtractLoad
from database.postgres import PostgresDB
import yaml



class TestRawExtract:

    """ Test all functions related to Extracts from the Raw Database """

    def setup_class(self):
        
        config_path = "crypto/config.yaml"

        # get yaml config 
        with open(config_path) as stream:
            config = yaml.safe_load(stream)


    def test_extract_trending_from_database(self):

        """ Test the extract_from_database function.
        
        The raw trending data is extracted into test_staging_trending in the staging database
        """

        table_name = "test_staging_trending"
        source_engine = PostgresDB.create_pg_engine("raw")
        path = "crypto/models/extract_staging/test_extract_staging"
        path_extract_log = "crypto/logs/extract_log"
        
        # map raw tables to stage tables
        table_mapping = {
            'test_staging_coin_history': 'test_raw_coin_history',
            'test_staging_coins': 'test_raw_coins',
            'test_staging_trending': 'test_raw_trending'
        }

        extract_df = Extract.extract_from_database(
            table_name=table_name, 
            engine=source_engine, 
            path=path, 
            path_extract_log=path_extract_log, 
            table_mapping=table_mapping
        )

        assert extract_df.shape[0] > 0


    def test_extract_coins_from_database(self):

        """ Test the extract_from_database function
        
        The raw coin data is extracted into test_staging_coins in the staging database
        """

        table_name = "test_staging_coins"
        source_engine = PostgresDB.create_pg_engine("raw")
        path = "crypto/models/extract_staging/test_extract_staging"
        path_extract_log = "crypto/logs/extract_log"

        # map raw tables to stage tables
        table_mapping = {
            'test_staging_coin_history': 'test_raw_coin_history',
            'test_staging_coins': 'test_raw_coins',
            'test_staging_trending': 'test_raw_trending'
        }

        extract_df = Extract.extract_from_database(
            table_name=table_name, 
            engine=source_engine, 
            path=path, 
            path_extract_log=path_extract_log,
            table_mapping=table_mapping
        )

        assert extract_df.shape[0] > 0



    def test_extract_coin_history_from_raw_database(self):

        """ 
        Test the extract_from_database function
        
        Toggle the config in test_staging_coin_history.sql to run in incremental or full mode 
        
        """

        table_name = "test_staging_coin_history"
        source_engine = PostgresDB.create_pg_engine("raw")
        path = "crypto/models/extract_staging/test_extract_staging"
        path_extract_log = "crypto/log/extract_log"

        
        # map raw tables to stage tables
        table_mapping = {
            'test_staging_coin_history': 'test_raw_coin_history',
            'test_staging_coins': 'test_raw_coins',
            'test_staging_trending': 'test_raw_trending'
        }

        extract_df = Extract.extract_from_database(
            table_name=table_name, 
            engine=source_engine, 
            path=path, 
            path_extract_log=path_extract_log,
            table_mapping=table_mapping
        )


        # assert extract_df.shape[0] == 20 # this will fail sometimes for incremental loads
        assert extract_df.shape[0] > 0 # replace 0 with the appropriate number of records



    def test_extract_load_coin_history(self):

        """ 
        Load the extracted coin history data from the raw database to the staging database using 
        the ExtractLoad.run method.
        
        """

        table_name = "test_staging_coin_history"
        source_engine = PostgresDB.create_pg_engine("raw")
        target_engine = PostgresDB.create_pg_engine("staging")
        path = "crypto/models/extract_staging/test_extract_staging"
        path_extract_log = "crypto/log/extract_log"

        # map raw tables to stage tables
        table_mapping = {
            'test_staging_coin_history': 'test_raw_coin_history',
            'test_staging_coins': 'test_raw_coins',
            'test_staging_trending': 'test_raw_trending'
        }

        result = ExtractLoad(
            table_name=table_name, 
            source_engine=source_engine,
            target_engine=target_engine,
            path=path, 
            path_extract_log=path_extract_log,
            table_mapping=table_mapping
        )

        assert result.run() == True


    def test_extract_load_trending(self):

        """ 
        Load the extracted coin trending data from the raw database to the staging database using 
        the ExtractLoad.run method.
        
        """

        table_name = "test_staging_trending"
        source_engine = PostgresDB.create_pg_engine("raw")
        target_engine = PostgresDB.create_pg_engine("staging")
        path = "crypto/models/extract_staging/test_extract_staging"
        path_extract_log = "crypto/log/extract_log"

        # map raw tables to stage tables
        table_mapping = {
            'test_staging_coin_history': 'test_raw_coin_history',
            'test_staging_coins': 'test_raw_coins',
            'test_staging_trending': 'test_raw_trending'
        }

        result = ExtractLoad(
            table_name=table_name, 
            source_engine=source_engine,
            target_engine=target_engine,
            path=path, 
            path_extract_log=path_extract_log,
            table_mapping=table_mapping
        )

        assert result.run() == True


    def test_extract_load_coin(self):

        """ 
        Load the extracted coin data from the raw database to the staging database using 
        the ExtractLoad.run method.
        
        """

        table_name = "test_staging_coins"
        source_engine = PostgresDB.create_pg_engine("raw")
        target_engine = PostgresDB.create_pg_engine("staging")
        path = "crypto/models/extract_staging/test_extract_staging"
        path_extract_log = "crypto/log/extract_log"

        # map raw tables to stage tables
        table_mapping = {
            'test_staging_coin_history': 'test_raw_coin_history',
            'test_staging_coins': 'test_raw_coins',
            'test_staging_trending': 'test_raw_trending'
        }

        result = ExtractLoad(
            table_name=table_name, 
            source_engine=source_engine,
            target_engine=target_engine,
            path=path, 
            path_extract_log=path_extract_log,
            table_mapping=table_mapping
        )

        assert result.run() == True
















