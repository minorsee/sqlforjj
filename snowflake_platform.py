from flask import Flask, render_template, request, jsonify
import sqlite3
from datetime import datetime, timedelta
import random
import uuid
import json
import re

app = Flask(__name__)

class SnowflakePlatform:
    def __init__(self, db_path='snowflake_gateway.db'):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Transactions table with Snowflake-like structure
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS TRANSACTIONS (
                TRANSACTION_ID TEXT PRIMARY KEY,
                MERCHANT_ID TEXT NOT NULL,
                CUSTOMER_ID TEXT NOT NULL,
                AMOUNT REAL NOT NULL,
                CURRENCY TEXT NOT NULL,
                STATUS TEXT NOT NULL,
                PAYMENT_METHOD TEXT NOT NULL,
                TRANSACTION_TIMESTAMP TEXT NOT NULL,
                COUNTRY TEXT NOT NULL,
                MERCHANT_CATEGORY TEXT NOT NULL,
                METADATA TEXT,
                CREATED_AT TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Merchants table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS MERCHANTS (
                MERCHANT_ID TEXT PRIMARY KEY,
                MERCHANT_NAME TEXT NOT NULL,
                BUSINESS_TYPE TEXT NOT NULL,
                COUNTRY TEXT NOT NULL,
                REGISTRATION_DATE TEXT NOT NULL,
                STATUS TEXT NOT NULL,
                MONTHLY_VOLUME REAL,
                CONTACT_INFO TEXT,
                CREATED_AT TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Customers table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS CUSTOMERS (
                CUSTOMER_ID TEXT PRIMARY KEY,
                EMAIL TEXT NOT NULL,
                COUNTRY TEXT NOT NULL,
                REGISTRATION_DATE TEXT NOT NULL,
                TOTAL_TRANSACTIONS INTEGER DEFAULT 0,
                LIFETIME_VALUE REAL DEFAULT 0,
                RISK_SCORE INTEGER NOT NULL,
                PROFILE_DATA TEXT,
                LAST_LOGIN TEXT,
                CREATED_AT TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Check if tables are empty and populate with sample data
        cursor.execute("SELECT COUNT(*) FROM TRANSACTIONS")
        if cursor.fetchone()[0] == 0:
            self.populate_sample_data(cursor)
        
        conn.commit()
        conn.close()

    def populate_sample_data(self, cursor):
        # Sample merchants with VARIANT data
        business_types = ['E-COMMERCE', 'SAAS', 'RETAIL', 'FOOD_BEVERAGE', 'GAMING', 'EDUCATION', 'HEALTHCARE']
        countries = ['USA', 'CANADA', 'UK', 'GERMANY', 'FRANCE', 'AUSTRALIA', 'JAPAN', 'BRAZIL']
        merchant_names = ['TechStore Pro', 'CloudSoft Inc', 'Fashion Hub', 'QuickEats', 'GameWorld', 'EduPlatform', 
                         'HealthCare Plus', 'BookStore Online', 'MusicStream', 'FitnessPro']
        
        merchants = []
        for i in range(50):
            merchant_id = f"MERCH_{str(uuid.uuid4())[:8].upper()}"
            name = f"{random.choice(merchant_names)} {i+1}"
            business_type = random.choice(business_types)
            country = random.choice(countries)
            reg_date = (datetime.now() - timedelta(days=random.randint(100, 1000))).date()
            status = random.choice(['ACTIVE', 'ACTIVE', 'ACTIVE', 'SUSPENDED', 'PENDING'])
            volume = round(random.uniform(10000, 500000), 2)
            
            # Snowflake VARIANT data (stored as JSON string)
            contact_info = json.dumps({
                "phone": f"+1-{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
                "website": f"https://{name.lower().replace(' ', '')}.com",
                "support_email": f"support@{name.lower().replace(' ', '')}.com"
            })
            
            created_at = datetime.now() - timedelta(days=random.randint(50, 800))
            
            merchants.append((merchant_id, name, business_type, country, reg_date, status, volume, contact_info, created_at))
        
        cursor.executemany('''
            INSERT INTO MERCHANTS (MERCHANT_ID, MERCHANT_NAME, BUSINESS_TYPE, COUNTRY, REGISTRATION_DATE, STATUS, MONTHLY_VOLUME, CONTACT_INFO, CREATED_AT)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', merchants)
        
        # Get merchant IDs
        cursor.execute("SELECT MERCHANT_ID FROM MERCHANTS")
        merchant_ids = [row[0] for row in cursor.fetchall()]
        
        # Sample customers with VARIANT profile data
        customers = []
        for i in range(200):
            customer_id = f"CUST_{str(uuid.uuid4())[:8].upper()}"
            email = f"customer{i+1}@email.com"
            country = random.choice(countries)
            reg_date = (datetime.now() - timedelta(days=random.randint(30, 800))).date()
            total_trans = random.randint(1, 50)
            lifetime_val = round(random.uniform(50, 5000), 2)
            risk_score = random.randint(1, 100)
            
            # Profile data as VARIANT
            profile_data = json.dumps({
                "age_group": random.choice(["18-25", "26-35", "36-45", "46-55", "56+"]),
                "preferred_payment": random.choice(["CREDIT_CARD", "DEBIT_CARD", "PAYPAL"]),
                "loyalty_tier": random.choice(["BRONZE", "SILVER", "GOLD", "PLATINUM"]),
                "marketing_consent": random.choice([True, False])
            })
            
            last_login = datetime.now() - timedelta(days=random.randint(1, 30), hours=random.randint(0, 23))
            created_at = datetime.now() - timedelta(days=random.randint(30, 600))
            
            customers.append((customer_id, email, country, reg_date, total_trans, lifetime_val, risk_score, profile_data, last_login, created_at))
        
        cursor.executemany('''
            INSERT INTO CUSTOMERS (CUSTOMER_ID, EMAIL, COUNTRY, REGISTRATION_DATE, TOTAL_TRANSACTIONS, LIFETIME_VALUE, RISK_SCORE, PROFILE_DATA, LAST_LOGIN, CREATED_AT)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', customers)
        
        # Get customer IDs
        cursor.execute("SELECT CUSTOMER_ID FROM CUSTOMERS")
        customer_ids = [row[0] for row in cursor.fetchall()]
        
        # Sample transactions with metadata
        statuses = ['SUCCESS', 'SUCCESS', 'SUCCESS', 'FAILED', 'PENDING', 'CANCELLED']
        payment_methods = ['CREDIT_CARD', 'DEBIT_CARD', 'PAYPAL', 'BANK_TRANSFER', 'APPLE_PAY', 'GOOGLE_PAY']
        currencies = ['USD', 'EUR', 'GBP', 'CAD', 'AUD', 'JPY']
        categories = ['ELECTRONICS', 'CLOTHING', 'FOOD', 'SOFTWARE', 'GAMES', 'BOOKS', 'HEALTH', 'TRAVEL']
        
        transactions = []
        for i in range(1000):
            transaction_id = f"TXN_{str(uuid.uuid4())[:12].upper()}"
            merchant_id = random.choice(merchant_ids)
            customer_id = random.choice(customer_ids)
            amount = round(random.uniform(5.99, 999.99), 2)
            currency = random.choice(currencies)
            status = random.choice(statuses)
            payment_method = random.choice(payment_methods)
            trans_timestamp = datetime.now() - timedelta(days=random.randint(1, 365), 
                                                        hours=random.randint(0, 23), 
                                                        minutes=random.randint(0, 59))
            country = random.choice(countries)
            category = random.choice(categories)
            
            # Transaction metadata as VARIANT
            metadata = json.dumps({
                "ip_address": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
                "user_agent": random.choice(["Chrome/91.0", "Firefox/89.0", "Safari/14.0"]),
                "device_type": random.choice(["DESKTOP", "MOBILE", "TABLET"]),
                "payment_processor": random.choice(["STRIPE", "PAYPAL", "SQUARE"]),
                "fraud_score": round(random.uniform(0, 1), 3)
            })
            
            created_at = trans_timestamp
            
            transactions.append((transaction_id, merchant_id, customer_id, amount, currency, 
                              status, payment_method, trans_timestamp, country, category, metadata, created_at))
        
        cursor.executemany('''
            INSERT INTO TRANSACTIONS (TRANSACTION_ID, MERCHANT_ID, CUSTOMER_ID, AMOUNT, CURRENCY, STATUS, 
                                    PAYMENT_METHOD, TRANSACTION_TIMESTAMP, COUNTRY, MERCHANT_CATEGORY, METADATA, CREATED_AT)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', transactions)

    def translate_snowflake_query(self, query):
        # Translate common Snowflake functions to SQLite equivalents
        translated = query
        
        # DATE_TRUNC function
        translated = re.sub(r"DATE_TRUNC\s*\(\s*'(\w+)'\s*,\s*([^)]+)\)", 
                           lambda m: self.convert_date_trunc(m.group(1), m.group(2)), 
                           translated, flags=re.IGNORECASE)
        
        # CURRENT_TIMESTAMP() to datetime('now')
        translated = re.sub(r"CURRENT_TIMESTAMP\(\)", "datetime('now')", translated, flags=re.IGNORECASE)
        
        # TRY_CAST to simple CAST
        translated = re.sub(r"TRY_CAST\s*\(", "CAST(", translated, flags=re.IGNORECASE)
        
        # ILIKE to LIKE (case insensitive)
        translated = re.sub(r"\bILIKE\b", "LIKE", translated, flags=re.IGNORECASE)
        
        # Handle table references (remove schema prefixes for SQLite)
        translated = re.sub(r"PAYMENT_DB\.PUBLIC\.", "", translated, flags=re.IGNORECASE)
        
        return translated

    def convert_date_trunc(self, unit, date_expr):
        unit = unit.upper()
        if unit == 'YEAR':
            return f"strftime('%Y-01-01', {date_expr})"
        elif unit == 'MONTH':
            return f"strftime('%Y-%m-01', {date_expr})"
        elif unit == 'DAY':
            return f"date({date_expr})"
        elif unit == 'HOUR':
            return f"strftime('%Y-%m-%d %H:00:00', {date_expr})"
        else:
            return f"date({date_expr})"

    def execute_query(self, query):
        try:
            # Translate Snowflake syntax to SQLite
            translated_query = self.translate_snowflake_query(query)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(translated_query)
            columns = [description[0] for description in cursor.description] if cursor.description else []
            results = cursor.fetchall()
            
            conn.close()
            
            return {
                'success': True,
                'columns': columns,
                'data': results,
                'row_count': len(results),
                'translated_query': translated_query if translated_query != query else None
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

platform = SnowflakePlatform()

@app.route('/')
def index():
    return render_template('snowflake_index.html')

@app.route('/execute', methods=['POST'])
def execute_sql():
    query = request.json.get('query', '').strip()
    
    if not query:
        return jsonify({'success': False, 'error': 'No query provided'})
    
    if not query.upper().startswith('SELECT'):
        return jsonify({'success': False, 'error': 'Only SELECT queries are allowed'})
    
    result = platform.execute_query(query)
    return jsonify(result)

@app.route('/schema')
def get_schema():
    conn = sqlite3.connect(platform.db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    schema_info = {}
    for table in tables:
        table_name = table[0].upper()  # Snowflake style uppercase
        cursor.execute(f"PRAGMA table_info({table[0]})")
        columns = cursor.fetchall()
        schema_info[f"PAYMENT_DB.PUBLIC.{table_name}"] = [
            {'name': col[1].upper(), 'type': self.map_to_snowflake_type(col[2])} 
            for col in columns
        ]
    
    conn.close()
    return jsonify(schema_info)

def map_to_snowflake_type(sqlite_type):
    mapping = {
        'TEXT': 'VARCHAR',
        'INTEGER': 'NUMBER',
        'REAL': 'NUMBER(10,2)',
        'BLOB': 'BINARY'
    }
    return mapping.get(sqlite_type.upper(), 'VARCHAR')

if __name__ == '__main__':
    app.run(debug=True)