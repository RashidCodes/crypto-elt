from crypto.elt.extract import Extract
from database.postgres import PostgresDB
from crypto.pipeline.extract_load_pipeline import ExtractLoad
import yaml


class TestStagingExtract:


    """ Test all functions related to Extracts from the Staging Database """

    def test_extract_trending_from_database(self):

        """ Test the extract_from_database function.
        
        The serving trending data is extracted into test_serving_trending in the serving database
        """

        table_name = "test_serving_trending"
        source_engine = PostgresDB.create_pg_engine("staging")
        path = "crypto/models/extract_serving/test_extract_serving"
        path_extract_log = "crypto/log/extract_log"
        
        # map raw tables to stage tables
        table_mapping = {
            'test_serving_coin_history': 'test_staging_coin_history',
            'test_serving_coins': 'test_staging_coins',
            'test_serving_trending': 'test_staging_trending'
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

        """ Test the extract_from_database function.
        
        The serving trending data is extracted into test_serving_trending in the serving database
        """

        table_name = "test_serving_coins"
        source_engine = PostgresDB.create_pg_engine("staging")
        path = "crypto/models/extract_serving/test_extract_serving"
        path_extract_log = "crypto/log/extract_log"
        
        # map raw tables to stage tables
        table_mapping = {
            'test_serving_coin_history': 'test_staging_coin_history',
            'test_serving_coins': 'test_staging_coins',
            'test_serving_trending': 'test_staging_trending'
        }

        extract_df = Extract.extract_from_database(
            table_name=table_name, 
            engine=source_engine, 
            path=path, 
            path_extract_log=path_extract_log, 
            table_mapping=table_mapping
        )

        assert extract_df.shape[0] > 0



    
    def test_extract_coin_history_from_database(self):

        """ Test the extract_from_database function.
        
        The serving coin history data is extracted into test_serving_coin_history in the serving database
        """

        table_name = "test_serving_coin_history"
        source_engine = PostgresDB.create_pg_engine("staging")
        path = "crypto/models/extract_serving/test_extract_serving"
        path_extract_log = "crypto/log/extract_log"
        
        # map raw tables to stage tables
        table_mapping = {
            'test_serving_coin_history': 'test_staging_coin_history',
            'test_serving_coins': 'test_staging_coins',
            'test_serving_trending': 'test_staging_trending'
        }

        extract_df = Extract.extract_from_database(
            table_name=table_name, 
            engine=source_engine, 
            path=path, 
            path_extract_log=path_extract_log, 
            table_mapping=table_mapping
        )

        assert extract_df.shape[0] > 0



    def test_extract_load_trending(self):

        """ 
        Load the extracted coin trending data from the staging database to the serving database using 
        the ExtractLoad.run method.
        
        """

        table_name = "test_serving_trending"
        source_engine = PostgresDB.create_pg_engine("staging")
        target_engine = PostgresDB.create_pg_engine("serving")
        path = "crypto/models/extract_serving/test_extract_serving"
        path_extract_log = "crypto/log/extract_log"

        # map raw tables to stage tables
        table_mapping = {
            'test_serving_coin_history': 'test_staging_coin_history',
            'test_serving_coins': 'test_staging_coins',
            'test_serving_trending': 'test_staging_trending'
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



    def test_extract_load_coins(self):

        """ 
        Load the extracted coin data from the staging database to the serving database using 
        the ExtractLoad.run method.
        
        """

        table_name = "test_serving_coins"
        source_engine = PostgresDB.create_pg_engine("staging")
        target_engine = PostgresDB.create_pg_engine("serving")
        path = "crypto/models/extract_serving/test_extract_serving"
        path_extract_log = "crypto/log/extract_log"

        # map raw tables to stage tables
        table_mapping = {
            'test_serving_coin_history': 'test_staging_coin_history',
            'test_serving_coins': 'test_staging_coins',
            'test_serving_trending': 'test_staging_trending'
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


    
    def test_extract_load_coin_history(self):

        """ 
        Load the extracted coin history data from the staging database to the serving database using 
        the ExtractLoad.run method.
        
        """

        table_name = "test_serving_coin_history"
        source_engine = PostgresDB.create_pg_engine("staging")
        target_engine = PostgresDB.create_pg_engine("serving")
        path = "crypto/models/extract_serving/test_extract_serving"
        path_extract_log = "crypto/log/extract_log"

        # map raw tables to stage tables
        table_mapping = {
            'test_serving_coin_history': 'test_staging_coin_history',
            'test_serving_coins': 'test_staging_coins',
            'test_serving_trending': 'test_staging_trending'
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