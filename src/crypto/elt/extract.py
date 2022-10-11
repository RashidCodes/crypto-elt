from turtle import back
import pandas as pd
import jinja2 as j2 
import logging 
import os 
import datetime as dt 
from pycoingecko import CoinGeckoAPI


cg = CoinGeckoAPI()

class Extract(): 

    @staticmethod
    def coins():
        
        raw_coins_df = pd.json_normalize(cg.get_coins_list(), max_level=0)
        raw_coins_df['update_timestamp'] = pd.to_datetime("today").date().strftime("%d-%m-%Y")

        return raw_coins_df

    

    @staticmethod
    def trending():

        trending_coins = [coin.get("item") for coin in cg.get_search_trending().get("coins")]
        raw_trends_df = pd.DataFrame(trending_coins)
        raw_trends_df['update_timestamp'] = pd.to_datetime("today").date().strftime("%d-%m-%Y")

        return raw_trends_df


    @staticmethod
    def coins_history(date_range: dict, list_of_coins = None) -> pd.DataFrame:

        """
        Extract raw coin data for a date range. Great for backfilling

        Parameters 
        ----------
        date_range: list 
            A dict with start_date and end_date as keys and dates as values. Dates are in the 
            '%d-%m-%Y' format. 
            
            Example: 
            date_range = {'start_date': '01-01-2008', 'end_date': '10-01-2008'}


        Returns
        -------
        coin_history_df: pd.DataFrame 
            Dataframe of coins history
        
        """

        coin_history_df = pd.DataFrame()

        if list_of_coins is None:
            list_of_coins= ['bitcoin','litecoin','ethereum','solana' ,'umee','terra-luna','evmos','dejitaru-tsuka','reserve-rights-token','insights-network']

        # parse the date strings
        start_date = dt.datetime.strptime(date_range.get("start_date"), '%d-%m-%Y')
        end_date = dt.datetime.strptime(date_range.get("end_date"), '%d-%m-%Y')

            
        while start_date <= end_date:

            for coin in list_of_coins:
                formatted_start_date = start_date.date().strftime('%d-%m-%Y')
                raw_data = cg.get_coin_history_by_id(id=coin, date=formatted_start_date, localization='false')
                raw_df = pd.json_normalize(raw_data, max_level=0)
                raw_df['price_date'] = start_date 
                coin_history_df = pd.concat([coin_history_df, raw_df])
                
            start_date = start_date + dt.timedelta(days=1)

        # add a key column
        coin_history_df['key'] = [i+1 for i in range(coin_history_df.shape[0])]

        
        return coin_history_df




    @staticmethod
    def get_incremental_value(table_name, path="extract_log"):
        df = pd.read_csv(f"{path}/{table_name}.csv")
        return df[df["log_date"] == df["log_date"].max()]["incremental_value"].values[0]



    @staticmethod
    def is_incremental(table:str, path:str)->bool:
        # read sql contents into a variable 
        with open(f"{path}/{table}.sql") as f: 
            raw_sql = f.read()
        try: 
            config = j2.Template(raw_sql).make_module().config 
            return config["extract_type"].lower() == "incremental"
        except:
            return False




    @staticmethod
    def upsert_incremental_log(log_path, table_name, incremental_value)->bool:
        if f"{table_name}.csv" in os.listdir(log_path):
            df_existing_incremental_log = pd.read_csv(f"{log_path}/{table_name}.csv")
            df_incremental_log = pd.DataFrame(data={
                "log_date": [dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")], 
                "incremental_value": [incremental_value]
            })
            df_updated_incremental_log = pd.concat([df_existing_incremental_log,df_incremental_log])
            df_updated_incremental_log.to_csv(f"{log_path}/{table_name}.csv", index=False)
        else: 
            df_incremental_log = pd.DataFrame(data={
                "log_date": [dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")], 
                "incremental_value": [incremental_value]
            })
            df_incremental_log.to_csv(f"{log_path}/{table_name}.csv", index=False)
        return True 



    @staticmethod 
    def extract_from_api(table_name: str, backfill_dates: dict = None, **kwargs):

        """
        Extract data from CoinGecko API. This function uses the name of the 
        extract queries (withouth .sql) to extract data. These extract queries 
        are later used to extract data from the raw layer.

        For example, if coins.sql exists in `path_extract_query`, data about coins is
        extracted from the CoinGecko API and stored in raw_coins. 


        Parameters
        -----------
        path_extract_query: str 
            File location of extract queries.

        table_name: str 
            Name of the table, for example, to store raw data about coins in the 
            raw layer, a table_name of "coins" should be used. The raw coin data is
            then stored in raw_coins.

        backfill_dates: datetime 
            If a backfill date is provided, data from the backfill_dates to the current
            date is extracted


        Returns 
        -------
        df: DataFrame 
            Extracted data 

        """

       

        # extract raw data and load into raw database
        if table_name == 'coin_history':
            
            if backfill_dates is not None:

                list_of_coins = kwargs.get('list_of_coins', None) 
                raw_coin_history_df = Extract.coins_history(backfill_dates, list_of_coins=list_of_coins)

                return raw_coin_history_df

            start_date = dt.datetime.today().date().strftime('%d-%m-%Y')
            end_date = dt.datetime.today().date().strftime('%d-%m-%Y')
            backfill_dates = {'start_date': start_date, 'end_date': end_date}

            raw_coin_history_df = Extract.coins_history(backfill_dates)

            return raw_coin_history_df


        if table_name == 'coins':
            coin_list_df = Extract.coins() 

            return coin_list_df


        if table_name == 'trending':
            trending_df = Extract.trending()

            return trending_df







    @staticmethod
    def extract_from_database(table_name, engine, path, path_extract_log, table_mapping: dict=None) -> pd.DataFrame:   

        """ 

        Extract data from a database


        Parameters
        ----------
        table_name: str 
            The name of the source table 

        engine: Postgres engine 

        path: str 
            Directory containing the extract queries.


        Returns
        -------
        extracted_df: DataFrame 
            Data frame containing data from the source table

        """


        logging.basicConfig(level=logging.INFO, format="[%(levelname)s][%(asctime)s]: %(message)s")

        if f"{table_name}.sql" in os.listdir(path):

            logging.info(f"Extracting table: {table_mapping.get(table_name)}")

            # read the sql contents 
            with open(f"{path}/{table_name}.sql") as f:
                raw_sql = f.read()

            
            
            # get the config from the extract query
            config  = j2.Template(raw_sql).make_module().config


            if config.get("extract_type").lower() == 'incremental':

                # we'll save the logs to the database later 
                # for now, they're saved in a csv file 
                if not os.path.exists(path_extract_log):
                    os.mkdir(path_extract_log)

                
                # get the watermark files for the table 
                if f"{table_name}.csv" in os.listdir(path_extract_log):

                    # get the incremental value and perform the incremental extract 
                    current_max_incremental_value = Extract.get_incremental_value(table_name, path=path_extract_log)

                    parsed_sql = j2.Template(raw_sql).render(
                        source_table_name = table_mapping.get(table_name),
                        engine = engine,
                        is_incremental = True,
                        incremental_value = current_max_incremental_value
                    )


                    # execute incremental extraction 
                    extracted_df = pd.read_sql(parsed_sql, con=engine)

                    # update the max incremental extract 
                    if extracted_df.shape[0] > 0:
                        max_incremental_value = extracted_df[config["incremental_column"]].max()

                    else:
                        max_incremental_value = current_max_incremental_value 


                    
                    # upsert the logs 
                    Extract.upsert_incremental_log(log_path=path_extract_log, table_name=table_name, incremental_value=max_incremental_value)
                    logging.info(f"Successfully extracted table: {table_mapping.get(table_name)} incrementally, rows extracted: {extracted_df.shape[0]}")

                    return extracted_df

                else:
                    # parse sql using jinja 
                    parsed_sql = j2.Template(raw_sql).render(source_table_name=table_mapping.get(table_name), engine=engine)

                    # perform full extract (first time only)
                    df = pd.read_sql(parsed_sql, con=engine)

                    # store the latest incremental value 
                    max_incremental_value = df[config["incremental_column"]].max()

                    Extract.upsert_incremental_log(log_path=path_extract_log, table_name=table_name, incremental_value=max_incremental_value)

                    logging.info(f"Successfully extracted table: {table_mapping.get(table_name)}, rows extracted: {df.shape[0]}")

                    return df

            else:
                # parse sql with jinja
                parsed_sql = j2.Template(raw_sql).render(source_table_name=table_mapping.get(table_name), engine=engine)

                # store extracted data 
                extracted_df = pd.read_sql(parsed_sql, con=engine)

                logging.info(f"Successfully extracted table: {table_mapping.get(table_name)}, rows extracted: {extracted_df.shape[0]}")

                return extracted_df 

        else:

            raise Exception(f"Could not find table: {table_mapping.get(table_name)}")


           