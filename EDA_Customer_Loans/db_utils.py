import yaml
from sqlalchemy import create_engine
import pandas as pd

class RDSDatabaseConnector:

    def load_credentials(file_path = "credentials.yaml"):
        with open(file_path, 'r') as file:
            credentials = yaml.safe_load(file)
        return credentials    
    
    def __init__(self, credentials):
        self.credentials = credentials
        self.engine = self._create_engine()

        self.db_type = credentials['RDS_DATABASE_TYPE']
        self.db_api = credentials['RDS_DBAPI']
        self.host = credentials['RDS_HOST']
        self.port = credentials['RDS_PORT']
        self.database = credentials['RDS_DATABASE']
        self.user = credentials['RDS_USER']
        self.password = credentials['RDS_PASSWORD']

        self.create_engine() 

    def _create_engine(self):
        self.engine = create_engine(f"{self.db_type}+{self.db_api}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}")


    def extract_data(self, table_name="loan_payments"):
        # Extract data from the RDS database and return as a Pandas DataFrame
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, self.engine)
        return df

    def save_to_csv(self, data, file_path="loan_payments.csv"):
        # Save data to a CSV file
        data.to_csv(file_path, index=False)
        print(f"Data saved to {file_path}")


def load_data_from_csv(file_path, num_rows=5):
        # Load data from CSV file into a Pandas DataFrame
        df = pd.read_csv(file_path)
        
        # Print shape of the data
        print(f"Data loaded from {file_path}. Shape: {df.shape}")
        
        # Print a sample of the data
        print(f"Sample of the data:")
        print(df.head(num_rows))
        
        return df