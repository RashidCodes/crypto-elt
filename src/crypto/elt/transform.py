import os 
import logging 
import jinja2 as j2 

class Transform():

    def __init__(self, model, engine, models_path="models", table_mapping: dict=None):
        self.model = model 
        self.engine = engine 
        self.models_path = models_path
        self.table_mapping = table_mapping
    
    
    def run(self)->bool:
        """
        Builds models with a matching file name in the models_path folder. 
        - `model`: the name of the model (without .sql)
        - `models_path`: the path to the models directory containing the sql files. defaults to `models`
        """
        if f"{self.model}.sql" in os.listdir(self.models_path):
            logging.info(f"Building model: {self.model}")
        
            # read sql contents into a variable 
            with open(f"{self.models_path}/{self.model}.sql") as f: 
                raw_sql = f.read()

            # parse sql using jinja 
            parsed_sql = j2.Template(raw_sql).render(
                source_table_name=self.table_mapping.get(self.model), 
                target_table_name=self.model
            )

            # execute parsed sql 
            result = self.engine.execute(parsed_sql)
            logging.info(f"Successfully built model: {self.model}, rows inserted/updated: {result.rowcount}")
            return True 
        else: 
            raise Exception(f"Could not find model: {self.model}")