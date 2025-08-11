import os
import zipfile
import requests
import psycopg2
from kaggle_data_loader import KaggleDataLoader
from dotenv import load_dotenv

load_dotenv()

class KaggleDataSetup:
    def __init__(self, db_config, kaggle_api_key, data_dir='data/'):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'transaction_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'Admin123')
        }
        self.kaggle_api_key = kaggle_api_key
        self.data_dir = data_dir
        self.loader = KaggleDataLoader(self.db_config, self.data_dir)
    
    def connect_db(self):
        return psycopg2.connect(**self.db_config)
    
    def setup_database(self):
        try:
            conn = self.connect_db()
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id SERIAL PRIMARY KEY,
                    account_number VARCHAR(50) UNIQUE,
                    account_type VARCHAR(20)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id SERIAL PRIMARY KEY,
                    from_account_id INTEGER REFERENCES accounts(id),
                    to_account_id INTEGER REFERENCES accounts(id),
                    amount DECIMAL(15,2),
                    transaction_type VARCHAR(20),
                    timestamp TIMESTAMP,
                    description TEXT,
                    is_anomaly BOOLEAN,
                    anomaly_score DECIMAL(5,2),
                    anomaly_reasons TEXT[],
                    status VARCHAR(20) DEFAULT 'pending'
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS anomaly_detections (
                    id SERIAL PRIMARY KEY,
                    transaction_id INTEGER REFERENCES transactions(id),
                    detection_method VARCHAR(50),
                    anomaly_score DECIMAL(5,2),
                    confidence DECIMAL(5,2),
                    detection_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
            print("Database schema created successfully!")
        except Exception as e:
            print(f"Error setting up database: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def download_kaggle_dataset(self, dataset='ieee-fraud-detection', file_name='train_transaction.csv.zip'):
        try:
            os.makedirs(self.data_dir, exist_ok=True)
            url = f"https://www.kaggle.com/c/{dataset}/download/{file_name}"
            headers = {'Authorization': f'Bearer {self.kaggle_api_key}'}
            response = requests.get(url, headers=headers)
            
            file_path = os.path.join(self.data_dir, file_name)
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            print(f"Dataset downloaded to {file_path}")
            return file_path
        except Exception as e:
            print(f"Error downloading dataset: {str(e)}")
            raise
    
    def load_data(self, file_path):
        self.loader.load_kaggle_data(file_path)
        print("Data loaded successfully into database!")
    
    def setup(self, dataset='ieee-fraud-detection', file_name='train_transaction.csv.zip'):
        self.setup_database()
        file_path = self.download_kaggle_dataset(dataset, file_name)
        self.load_data(file_path)