from crypto.elt.extract import Extract
from crypto.elt.load import Load

class ExtractLoad():

    def __init__(self, target_engine, table_name, source_engine=None, path=None, path_extract_log=None, chunksize:int=1000, api: bool = False, test: bool = False, **kwargs):
        self.source_engine = source_engine
        self.target_engine = target_engine
        self.table_name = table_name
        self.path = path 
        self.path_extract_log = path_extract_log
        self.chunksize = chunksize
        self.api = api
        self.test = test 
        self.kwargs = kwargs



    def run(self):


        if self.api:
            
            # extract raw data from the API
            raw_df = Extract.extract_from_api(table_name=self.table_name, **self.kwargs)

            if self.test:

                # test mode 
                # load the test raw data into the raw database 
                try:
                    sample_raw_coins_df = raw_df

                    Load.overwrite_to_database(sample_raw_coins_df, table_name="test_raw_" + self.table_name, engine=self.target_engine)
                except BaseException as err:
                    print(f"An error occurred: {err}")
                    return False 
                else:
                    return True 

            else:
                # load the raw data into the raw database 
                try:

                    # TODO: Add incremental extraction for the raw data
                    Load.overwrite_to_database(raw_df, table_name="raw_" + self.table_name, engine=self.target_engine)
                except BaseException as err:
                    print(f"An error occurred: {err}")
                    return False 
                else:
                    return True 


        else:

            # extract data from a database 
            extract_df = Extract.extract_from_database(
                table_name=self.table_name, 
                engine=self.source_engine, 
                path=self.path, 
                path_extract_log=self.path_extract_log,
                table_mapping=self.kwargs.get("table_mapping")
            )

        

            #Load table  
            if len(extract_df) > 0:
                if Extract.is_incremental(table=self.table_name, path=self.path):
                    key_columns = Load.get_key_columns(table=self.table_name, path=self.path)

                    try:
                        Load.upsert_to_database(df=extract_df, table_name=self.table_name, key_columns=key_columns, engine=self.target_engine, chunksize=self.chunksize )
                    except BaseException as err:
                        print(f"An error occurred for incremental load: {err}")
                        return False 
                    else:
                        return True 

                    
                else:
                    try:
                        Load.overwrite_to_database(extract_df,table_name=self.table_name, engine=self.target_engine)
                    except BaseException as err:
                        print(f"An error occurred: {err}")
                        return False 
                    else:
                        return True 

            else:
                print("Extract.extract_from_database returned 0 rows")
                return True
            
