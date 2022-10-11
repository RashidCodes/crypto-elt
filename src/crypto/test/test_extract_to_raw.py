from crypto.elt.extract import Extract
from crypto.pipeline.extract_load_pipeline import ExtractLoad
from database.postgres import PostgresDB



class TestAPIExtract:

    """ Test all functions related to Extracts from the CoinGecko API """

    def test_extract_coin_history(self):

        # provide a start and end date 
        date_range = {'start_date': '01-01-2022', 'end_date': '01-01-2022'}
        list_of_coins = ['bitcoin', 'litecoin']

        history_df = Extract.coins_history(date_range, list_of_coins)

        assert history_df.shape[0] > 1




    def test_extract_from_api(self):

        """ Extract coin history using the extract_from_api method """

        # Get raw coin history using the extract_from_api function 
        raw_coin_history_df = Extract.extract_from_api(table_name = 'coin_history')

        # make sure data is returned
        assert raw_coin_history_df.shape[0] > 0 



    def test_extract_from_api_with_dates(self):

        """ 
        Extract coin history using the extract_from_api method with extra keyword arguments. 

        For example, list_of_coins = ['bitcoin'] and backfill_dates are also provided

        """

        backfill_dates = {'start_date': '01-01-2022', 'end_date': '01-01-2022'}
        list_of_coins = ['bitcoin']
        raw_coin_history_df = Extract.extract_from_api(table_name='coin_history', backfill_dates=backfill_dates, list_of_coins=list_of_coins)

        assert raw_coin_history_df.shape[0] > 0



    def test_extract_from_api_coins(self):

        """ Extract the coins list using the extract_from_api method """ 

        coins_list_df = Extract.extract_from_api(table_name='coins') 

        assert coins_list_df.shape[0] > 0 



    def test_extract_from_api_trending(self):

        """ Extract Top-7 trending coins on CoinGecko as searched by users in the last 24 hours (Ordered by most popular first) """ 

        trending_df = Extract.extract_from_api(table_name='trending')

        assert trending_df.shape[0] > 0 



    def test_extract_load_coins_from_api(self):

        """ Extract raw coins data from CoinGecko """

        target_engine = PostgresDB.create_pg_engine(db_target="raw")
        result = ExtractLoad(table_name="coins", target_engine=target_engine, api=True, test=True)

        assert result.run() == True 



    def test_extract_load_trending_from_api(self):

        """ Extract raw coins data from CoinGecko """

        target_engine = PostgresDB.create_pg_engine(db_target="raw")
        result = ExtractLoad(table_name="trending", target_engine=target_engine, api=True, test=True)
        
        assert result.run() == True 



    def test_extract_load_coin_history_from_api(self):

        """ Extract raw coins history data from CoinGecko """

        target_engine = PostgresDB.create_pg_engine(db_target="raw")
        result = ExtractLoad(table_name="coin_history", target_engine=target_engine, api=True, test=True)
        
        assert result.run() == True 



    def test_extract_load_coin_history_from_api_with_kwargs(self):

        """ Extract raw coins history data from CoinGecko with keyword arguments - backfill_dates and list_of_coins"""

        target_engine = PostgresDB.create_pg_engine(db_target="raw")
        backfill_dates = {'start_date': '01-01-2022', 'end_date': '04-01-2022'}
        list_of_coins = ['bitcoin','litecoin','ethereum','solana' ,'umee']
        result = ExtractLoad(   
            table_name="coin_history", 
            target_engine=target_engine, 
            api=True, 
            test=True, 
            backfill_dates=backfill_dates, 
            list_of_coins=list_of_coins
        )
        
        assert result.run() == True 

        








