import pandas as pd
import psycopg2
import zipfile
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class KaggleDataLoader:
    def __init__(self, db_config, data_path='data/'):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'transaction_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'Admin123')
        }
        self.data_path = data_path
    
    def connect_db(self):
        return psycopg2.connect(**self.db_config)
    
    def load_kaggle_data(self, file_path):
        try:
            if file_path.endswith('.zip'):
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(self.data_path)
                file_path = os.path.join(self.data_path, 'train_transaction.csv')
            
            chunksize = 10000
            for chunk in pd.read_csv(file_path, chunksize=chunksize):
                self.process_chunk(chunk)
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            raise
    
    def process_chunk(self, df):
        df = df.rename(columns={
            'TransactionID': 'id',
            'TransactionAmt': 'amount',
            'TransactionDT': 'timestamp',
            'ProductCD': 'transaction_type',
            'card1': 'from_account',
            'card2': 'to_account'
        })
        
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df['is_anomaly'] = df.get('isFraud', 0).astype(bool)
        df['anomaly_score'] = 0.0
        df['anomaly_reasons'] = [[] for _ in range(len(df))]
        df['status'] = 'pending'
        
        try:
            conn = self.connect_db()
            cursor = conn.cursor()
            
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO accounts (account_number, account_type)
                    VALUES (%s, %s)
                    ON CONFLICT (account_number) DO NOTHING
                """, (str(row['from_account']), 'unknown'))
                cursor.execute("""
                    INSERT INTO accounts (account_number, account_type)
                    VALUES (%s, %s)
                    ON CONFLICT (account_number) DO NOTHING
                """, (str(row['to_account']), 'unknown'))
                
                cursor.execute("""
                    SELECT id FROM accounts WHERE account_number = %s
                """, (str(row['from_account']),))
                from_account_id = cursor.fetchone()[0]
                
                cursor.execute("""
                    SELECT id FROM accounts WHERE account_number = %s
                """, (str(row['to_account']),))
                to_account_id = cursor.fetchone()[0]
                
                cursor.execute("""
                    INSERT INTO transactions (
                        id, from_account_id, to_account_id, amount, transaction_type,
                        timestamp, description, is_anomaly, anomaly_score, anomaly_reasons, status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['id'], from_account_id, to_account_id, float(row['amount']),
                    row['transaction_type'], row['timestamp'], 'Kaggle transaction',
                    row['is_anomaly'], row['anomaly_score'], row['anomaly_reasons'], row['status']
                ))
            
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error processing chunk: {str(e)}")
            raise