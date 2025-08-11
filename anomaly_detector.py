import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.preprocessing import StandardScaler
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

class TransactionAnomalyDetector:
    def __init__(self, db_config):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'transaction_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'port': os.getenv('DB_PORT', '5432')
        }
        self.scaler = StandardScaler()
        self.isolation_forest = IsolationForest(contamination=0.01, random_state=42)
        self.random_forest = RandomForestClassifier(random_state=42)
    
    def connect_db(self):
        return psycopg2.connect(**self.db_config)
    
    def load_data(self, limit=10000):
        try:
            conn = self.connect_db()
            query = """
                SELECT t.id, t.amount, t.transaction_type, t.timestamp,
                       a1.account_number as from_account, a2.account_number as to_account
                FROM transactions t
                JOIN accounts a1 ON t.from_account_id = a1.id
                JOIN accounts a2 ON t.to_account_id = a2.id
                LIMIT %s
            """
            df = pd.read_sql(query, conn, params=(limit,))
            conn.close()
            return df
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            raise
    
    def preprocess_data(self, df):
        df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
        df['day_of_week'] = pd.to_datetime(df['timestamp']).dt.dayofweek
        df['amount_scaled'] = self.scaler.fit_transform(df[['amount']])
        
        features = ['amount_scaled', 'hour', 'day_of_week']
        X = df[features].fillna(0)
        return X, df['id']
    
    def detect_statistical_anomalies(self, X):
        anomaly_scores = np.abs(X - X.mean()) / X.std()
        return anomaly_scores.max(axis=1)
    
    def detect_ml_anomalies(self, X):
        return -self.isolation_forest.fit_predict(X)
    
    def detect_network_anomalies(self, df):
        try:
            conn = self.connect_db()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT t.from_account_id, t.to_account_id, COUNT(*) as tx_count
                FROM transactions t
                GROUP BY t.from_account_id, t.to_account_id
                HAVING COUNT(*) > 5
            """)
            suspicious_pairs = cursor.fetchall()
            conn.close()
            
            # Get account numbers for the suspicious IDs
            conn = self.connect_db()
            cursor = conn.cursor()
            suspicious_accounts = set()
            for from_id, to_id, _ in suspicious_pairs:
                cursor.execute("SELECT account_number FROM accounts WHERE id = %s", (from_id,))
                from_account = cursor.fetchone()
                if from_account:
                    suspicious_accounts.add(from_account[0])
                
                cursor.execute("SELECT account_number FROM accounts WHERE id = %s", (to_id,))
                to_account = cursor.fetchone()
                if to_account:
                    suspicious_accounts.add(to_account[0])
            
            conn.close()
            return df['from_account'].isin(suspicious_accounts) | df['to_account'].isin(suspicious_accounts)
        except Exception as e:
            print(f"Error in network analysis: {str(e)}")
            return pd.Series([False] * len(df))
    
    def train_supervised_model(self, X, y):
        self.random_forest.fit(X, y)
    
    def detect_supervised_anomalies(self, X):
        return self.random_forest.predict_proba(X)[:, 1]
    
    def run_detection(self):
        df = self.load_data()
        X, ids = self.preprocess_data(df)
        
        stat_scores = self.detect_statistical_anomalies(X)
        ml_scores = self.detect_ml_anomalies(X)
        network_anomalies = self.detect_network_anomalies(df)
        
        # Mock supervised labels for demonstration
        y = (stat_scores > 3) | (ml_scores > 0) | network_anomalies
        self.train_supervised_model(X, y)
        supervised_scores = self.detect_supervised_anomalies(X)
        
        ensemble_scores = (stat_scores * 0.3 + ml_scores * 0.3 + supervised_scores * 0.4)
        anomalies = ensemble_scores > ensemble_scores.mean() + 2 * ensemble_scores.std()
        
        try:
            conn = self.connect_db()
            cursor = conn.cursor()
            
            for idx, is_anomaly in enumerate(anomalies):
                if is_anomaly:
                    transaction_id = ids.iloc[idx]
                    cursor.execute("""
                        UPDATE transactions
                        SET is_anomaly = %s,
                            anomaly_score = %s,
                            anomaly_reasons = %s,
                            detection_time = %s
                        WHERE id = %s
                    """, (
                        True,
                        float(ensemble_scores[idx]),
                        ['High amount', 'Unusual pattern'],
                        datetime.now(),  # new field
                        transaction_id
                    ))

                    
                    cursor.execute("""
                        INSERT INTO anomaly_detections (transaction_id, detection_method, anomaly_score, confidence, detection_time)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (transaction_id, 'ensemble', float(ensemble_scores[idx]), 0.95, datetime.now()))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return [{"transaction_id": str(ids.iloc[i]), "score": float(ensemble_scores[i])} for i in range(len(anomalies)) if anomalies[i]]
        except Exception as e:
            print(f"Error saving anomalies: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            raise