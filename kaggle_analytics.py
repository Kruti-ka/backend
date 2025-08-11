import pandas as pd
import psycopg2
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
import os

load_dotenv()

class KaggleAnalytics:
    def __init__(self, db_config):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'transaction_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'Admin123')
        }
    
    def connect_db(self):
        return psycopg2.connect(**self.db_config)
    
    def load_data(self, limit=10000):
        try:
            conn = self.connect_db()
            query = """
                SELECT t.id, t.amount, t.transaction_type, t.timestamp, t.is_anomaly, t.anomaly_score,
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
    
    def fraud_distribution(self):
        df = self.load_data()
        fraud_counts = df['is_anomaly'].value_counts()
        fig = px.pie(
            values=fraud_counts.values,
            names=['Non-Fraud', 'Fraud'] if len(fraud_counts) == 2 else ['Non-Fraud'],
            title='Fraud Distribution'
        )
        return fig.to_dict()
    
    def temporal_fraud_patterns(self, days=30):
        df = self.load_data()
        df['date'] = pd.to_datetime(df['timestamp']).dt.date
        df = df[df['timestamp'] >= datetime.now() - pd.Timedelta(days=days)]
        
        daily_fraud = df.groupby('date')['is_anomaly'].agg(['count', 'sum']).reset_index()
        daily_fraud['fraud_rate'] = daily_fraud['sum'] / daily_fraud['count']
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=daily_fraud['date'],
            y=daily_fraud['count'],
            name='Total Transactions',
            line=dict(color='blue')
        ))
        fig.add_trace(go.Scatter(
            x=daily_fraud['date'],
            y=daily_fraud['sum'],
            name='Fraudulent Transactions',
            line=dict(color='red')
        ))
        fig.update_layout(title='Temporal Fraud Patterns', xaxis_title='Date', yaxis_title='Count')
        return fig.to_dict()
    
    def account_risk_analysis(self):
        df = self.load_data()
        account_risk = df.groupby('from_account').agg({
            'is_anomaly': 'sum',
            'amount': 'mean',
            'anomaly_score': 'mean'
        }).reset_index()
        account_risk['risk_score'] = account_risk['is_anomaly'] * 0.5 + account_risk['anomaly_score'] * 0.5
        
        fig = px.bar(
            account_risk.sort_values('risk_score', ascending=False).head(10),
            x='from_account',
            y='risk_score',
            title='Top 10 High-Risk Accounts'
        )
        return fig.to_dict()
    
    def generate_report(self):
        report = {
            'fraud_distribution': self.fraud_distribution(),
            'temporal_patterns': self.temporal_fraud_patterns(),
            'account_risk': self.account_risk_analysis()
        }
        return report