import networkx as nx
import pandas as pd
import psycopg2
from datetime import datetime
from typing import Dict, List
import os
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class NetworkAnalyzer:
    def __init__(self, db_config=None):
        self.db_config = db_config or {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'transaction_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'port': os.getenv('DB_PORT', '5432')
        }
        self.G = nx.DiGraph()

    def connect_db(self):
        try:
            return psycopg2.connect(**self.db_config)
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            raise

    def load_network_data(self, days=None):
        try:
            conn = self.connect_db()
            # Removed date filter to include all historical data
            query = """
                SELECT t.from_account_id, t.to_account_id, t.amount, t.is_anomaly, t.anomaly_score,
                       t.transaction_type, a1.account_number as from_account, a2.account_number as to_account
                FROM transactions t
                JOIN accounts a1 ON t.from_account_id = a1.id
                JOIN accounts a2 ON t.to_account_id = a2.id
                ORDER BY t.timestamp DESC
                LIMIT 2000
            """
            df = pd.read_sql(query, conn)
            logger.info(f"Fetched {len(df)} transactions for network data (all historical data)")
            conn.close()
            
            for _, row in df.iterrows():
                self.G.add_edge(
                    row['from_account'], row['to_account'],
                    weight=row['amount'],
                    is_anomaly=row['is_anomaly'],
                    anomaly_score=row['anomaly_score'],
                    transaction_type=row['transaction_type']
                )
        except Exception as e:
            logger.error(f"Error loading network data: {str(e)}")
            raise

    def detect_circular_transactions(self):
        try:
            cycles = list(nx.simple_cycles(self.G))
            suspicious_transactions = []
            for cycle in cycles:
                for i in range(len(cycle)):
                    source = cycle[i]
                    target = cycle[(i + 1) % len(cycle)]
                    edge_data = self.G[source][target]
                    suspicious_transactions.append({
                        'source': source,
                        'target': target,
                        'amount': edge_data['weight'],
                        'is_anomaly': edge_data['is_anomaly'],
                        'anomaly_score': edge_data['anomaly_score'],
                        'transaction_type': edge_data['transaction_type']
                    })
            logger.info(f"Detected {len(suspicious_transactions)} circular transactions")
            return suspicious_transactions
        except Exception as e:
            logger.error(f"Error detecting circular transactions: {str(e)}")
            return []

    def detect_high_degree_nodes(self, threshold=10):
        try:
            high_degree_nodes = [
                node for node, degree in self.G.degree() if degree > threshold
            ]
            logger.info(f"Detected {len(high_degree_nodes)} high-degree nodes")
            return high_degree_nodes
        except Exception as e:
            logger.error(f"Error detecting high degree nodes: {str(e)}")
            return []

    def generate_network_report(self):
        try:
            self.load_network_data()
            report = {
                'num_nodes': self.G.number_of_nodes(),
                'num_edges': self.G.number_of_edges(),
                'circular_transactions': self.detect_circular_transactions(),
                'high_degree_nodes': self.detect_high_degree_nodes()
            }
            logger.info(f"Generated network report: {report}")
            return report
        except Exception as e:
            logger.error(f"Error generating network report: {str(e)}")
            return {
                'num_nodes': 0,
                'num_edges': 0,
                'circular_transactions': [],
                'high_degree_nodes': []
            }

    def get_network_data(self, days=30) -> Dict[str, List]:
        try:
            conn = self.connect_db()
            # Fetch accounts
            accounts_query = """
                SELECT id, account_number, balance, is_suspicious, risk_score, account_type
                FROM accounts
                WHERE account_number IS NOT NULL AND account_number != ''
            """
            accounts_df = pd.read_sql(accounts_query, conn)
            logger.info(f"Fetched {len(accounts_df)} accounts")
            
            # Filter out any rows with null account_number
            accounts_df = accounts_df.dropna(subset=['account_number'])
            
            nodes = []
            for _, row in accounts_df.iterrows():
                try:
                    # Ensure account_number is a valid string
                    account_number = str(row["account_number"]).strip()
                    if not account_number:
                        continue
                        
                    node = {
                        "id": account_number,
                        "label": f"{row['account_type'] or 'Account'} {account_number}",
                        "type": str(row["account_type"] or "unknown").strip(),
                        "balance": float(row["balance"]) if pd.notnull(row["balance"]) else 0.0,
                        "is_suspicious": bool(row["is_suspicious"]) if pd.notnull(row["is_suspicious"]) else False,
                        "risk_score": float(row["risk_score"]) if pd.notnull(row["risk_score"]) else 0.0
                    }
                    nodes.append(node)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping invalid account row: {e}")
                    continue

            # Fetch transactions with proper JOIN - removed date filter to include all historical data
            transactions_query = """
                SELECT t.from_account_id, t.to_account_id, t.amount, t.is_anomaly, t.anomaly_score, t.transaction_type,
                       a1.account_number as from_account, a2.account_number as to_account
                FROM transactions t
                JOIN accounts a1 ON t.from_account_id = a1.id
                JOIN accounts a2 ON t.to_account_id = a2.id
                WHERE a1.account_number IS NOT NULL AND a1.account_number != ''
                  AND a2.account_number IS NOT NULL AND a2.account_number != ''
                ORDER BY t.timestamp DESC
                LIMIT 2000
            """
            transactions_df = pd.read_sql(transactions_query, conn)
            logger.info(f"Fetched {len(transactions_df)} transactions (all historical data)")
            
            # Filter out any rows with null account numbers
            transactions_df = transactions_df.dropna(subset=['from_account', 'to_account'])
            
            edges = []
            for _, row in transactions_df.iterrows():
                try:
                    # Ensure account numbers are valid strings
                    from_account = str(row["from_account"]).strip()
                    to_account = str(row["to_account"]).strip()
                    
                    if not from_account or not to_account:
                        continue
                        
                    edge = {
                        "source": from_account,
                        "target": to_account,
                        "width": float(row["amount"]) / 1000 if pd.notnull(row["amount"]) else 1.0,
                        "is_anomaly": bool(row["is_anomaly"]) if pd.notnull(row["is_anomaly"]) else False,
                        "anomaly_score": float(row["anomaly_score"]) if pd.notnull(row["anomaly_score"]) else 0.0,
                        "transaction_type": str(row["transaction_type"] or "unknown").strip()
                    }
                    edges.append(edge)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping invalid transaction row: {e}")
                    continue

            conn.close()
            
            # Final validation - ensure no empty data
            if not nodes or not edges:
                logger.warning("No valid nodes or edges found, returning empty data")
                return {"nodes": [], "edges": []}
                
            # Log sample data for debugging
            if nodes:
                logger.info(f"Sample node: {nodes[0]}")
            if edges:
                logger.info(f"Sample edge: {edges[0]}")
                
            logger.info(f"Returning {len(nodes)} nodes and {len(edges)} edges")
            return {"nodes": nodes, "edges": edges}
            
        except Exception as e:
            logger.error(f"Error in get_network_data: {str(e)}")
            return {"nodes": [], "edges": []}