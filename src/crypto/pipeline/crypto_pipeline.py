from crypto.pipeline.extract_load_pipeline import ExtractLoad
from graphlib import TopologicalSorter
from database.postgres import PostgresDB
from crypto.elt.transform import Transform
from io import StringIO
from utility.metadata_logging import MetadataLogging
import yaml 
import datetime as dt 
import os 
import logging


def run_pipeline(backfill_dates: dict= None, list_of_coins: list=None):

    # set up logging 
    run_log = StringIO()
    logging.basicConfig(stream=run_log,level=logging.INFO, format="[%(levelname)s][%(asctime)s]: %(message)s")

    # set up metadata logger 
    # logs are stored in the staging database
    metadata_logger = MetadataLogging(db_target="staging")

    logging.info("Getting config variables")

    # configure pipeline 
    with open("crypto/config.yaml") as stream:
        config = yaml.safe_load(stream)
    
    path_extract_raw_staging = config["extract_staging"]["model_path"]
    path_extract_staging_serving = config["extract_serving"]["model_path"]
    path_extract_log = config["extract_staging"]["log_path"]
    path_transform_model = config["transform"]["model_path"]
    chunksize = config["load"]["chunksize"]


    metadata_log_table = config["meta"]["log_table"]
    metadata_log_run_id = metadata_logger.get_latest_run_id(db_table=metadata_log_table)
    metadata_logger.log(
        run_timestamp=dt.datetime.now(),
        run_status="started",
        run_id=metadata_log_run_id, 
        run_config=config,
        db_table=metadata_log_table
    )

    # set up databases
    raw_engine = PostgresDB.create_pg_engine(db_target="raw")
    staging_engine = PostgresDB.create_pg_engine(db_target="staging")
    serving_engine = PostgresDB.create_pg_engine(db_target="serving")

    try:

        # build dag 
        dag = TopologicalSorter()

        # These nodes extract raw data to the staging database
        # i.e. ExtractLoad to Staging
        raw_to_staging = []

        # These nodes extract staged data to the serving database
        # i.e. ExtractLoad to serving
        staging_to_serving = []

        logging.info("Creating extract and load nodes")

        # keys are the model names eg. staging_coin_history.sql (found in crypto/extract_staging/prod_extract_staging)
        # values are the corresponding source table name e.g. coin history in the 
        # raw database is called "raw_coin_history".
        # The source tables are passed into the extract models
        raw_stage_table_mapping = {
            'staging_coin_history': 'raw_coin_history',
            'staging_coins': 'raw_coins',
            'staging_trending': 'raw_trending'
        }

        stage_serving_table_mapping = {
            'serving_coin_history': 'staging_coin_history',
            'serving_coins': 'staging_coins',
            'serving_trending': 'staging_trending'
        }

        serving_serving_mapping = {
            'served_coins_history': 'serving_coin_history'
        }


        # ---------------
        #  NODE CREATION 
        # ---------------

        # There are three types of Nodes 
        # 1. Nodes that extract from the CoinGecko API 
        # 2. Nodes that extract from a database
        # 3. Nodes that transform the served data 

        
        
        # Extract coin history data from the CoinGecko API 
        coin_history_from_api = ExtractLoad(
            table_name="coin_history",
            target_engine=raw_engine,
            api=True,
            backfill_dates=backfill_dates if backfill_dates is not None else None,
            list_of_coins=list_of_coins
        )

        # Extract trending coins from CoinGecko API
        trending_coin_from_api = ExtractLoad(table_name="trending", target_engine=raw_engine, api=True)

        # Extract coin data from CoinGecko API
        coins_from_api = ExtractLoad(table_name="coins", target_engine=raw_engine, api=True)



        # Extract from raw DB to staging DB
        for file in os.listdir(path_extract_raw_staging):
            node_extract_load = ExtractLoad(
                source_engine=raw_engine, 
                target_engine=staging_engine,
                table_name=file.replace(".sql", ""), 
                path=path_extract_raw_staging, 
                path_extract_log=path_extract_log,
                chunksize=chunksize,
                table_mapping=raw_stage_table_mapping
            )
            
            raw_to_staging.append(node_extract_load)


        # Extract from staging DB to serving DB
        for file in os.listdir(path_extract_staging_serving):
            node_extract_load = ExtractLoad(
                source_engine=staging_engine, 
                target_engine=serving_engine,
                table_name=file.replace(".sql", ""), 
                path=path_extract_staging_serving, 
                path_extract_log=path_extract_log,
                chunksize=chunksize,
                table_mapping=stage_serving_table_mapping
            )

            staging_to_serving.append(node_extract_load)
        

        logging.info("Creating transform nodes")



        # TODO: Add Transform Nodes

        # node_staging_coins = Transform("staging_coins", engine=target_engine, models_path=path_transform_model)
        # node_staging_coins_history = Transform("staging_coins_history", engine=target_engine, models_path=path_transform_model)
        node_coin_history_tranform = Transform(
            "served_coins_history", 
            engine=serving_engine, 
            models_path=path_transform_model,
            table_mapping = serving_serving_mapping
        )

        # Populate dag with nodes
        dag.add(trending_coin_from_api, coins_from_api, coin_history_from_api)
        dag.add(*staging_to_serving, *raw_to_staging, coin_history_from_api)
        dag.add(node_coin_history_tranform, *staging_to_serving)

        logging.info("Executing DAG")


        # run dag 
        dag_rendered = tuple(dag.static_order())
        for node in dag_rendered: 
            node.run()

        logging.info("Pipeline run successful")
        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="completed",
            run_id=metadata_log_run_id, 
            run_config=config,
            run_log=run_log.getvalue(),
            db_table=metadata_log_table
        )

    except Exception as e:
        logging.exception(e)
        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="error",
            run_id=metadata_log_run_id, 
            run_config=config,
            run_log=run_log.getvalue(),
            db_table=metadata_log_table
        )

    print(run_log.getvalue())


if __name__ == "__main__":
    backfill_dates = {'start_date': '01-01-2022', 'end_date': '01-01-2022'}
    run_pipeline(backfill_dates=backfill_dates)