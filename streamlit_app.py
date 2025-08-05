import streamlit as st
import sqlite3
from datetime import datetime, timedelta
import random
import uuid
import json
import re
import pandas as pd

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

# Initialize platform
if 'platform' not in st.session_state:
    st.session_state.platform = SnowflakePlatform()

# Initialize password states
if 'unlocked_sections' not in st.session_state:
    st.session_state.unlocked_sections = set()

# Page configuration
st.set_page_config(
    page_title="🥵 Hotflake Janice SQL Learning Platform",
    page_icon="❄️",
    layout="wide"
)

# Header
st.markdown("""
<div style="text-align: center; padding: 20px; background: linear-gradient(45deg, #2196F3, #21CBF3); color: white; border-radius: 10px; margin-bottom: 20px;">
    <h1>🥵 Hotflake Janice SQL Learning Platform</h1>
    <p>Learn Snowflake SQL with JJ Janice</p>
</div>
""", unsafe_allow_html=True)

# Available Tables
st.header("📋 Available Tables")
st.markdown("""
**Ready to Query:**
- **PAYMENT_DB.PUBLIC.TRANSACTIONS** - Payment transaction records
- **PAYMENT_DB.PUBLIC.MERCHANTS** - Business account information
- **PAYMENT_DB.PUBLIC.CUSTOMERS** - Customer profile data
""")

# Example Queries Section
with st.expander("💡 Show Example Queries", expanded=False):
    # Password info
    st.info("🔑 **Password for locked sections:** `ilovejj`")
    
    # Basic WHERE Queries
    st.subheader("Basic WHERE Queries")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("SELECT * FROM TRANSACTIONS WHERE STATUS = 'SUCCESS'", key="basic1"):
            st.session_state.query = "SELECT * FROM PAYMENT_DB.PUBLIC.TRANSACTIONS WHERE STATUS = 'SUCCESS'"
        if st.button("SELECT * FROM TRANSACTIONS WHERE AMOUNT > 100", key="basic2"):
            st.session_state.query = "SELECT * FROM PAYMENT_DB.PUBLIC.TRANSACTIONS WHERE AMOUNT > 100"
    
    with col2:
        if st.button("WHERE with AND condition", key="basic3"):
            st.session_state.query = "SELECT * FROM PAYMENT_DB.PUBLIC.TRANSACTIONS WHERE STATUS = 'SUCCESS' AND AMOUNT > 100"
        if st.button("WHERE with OR condition", key="basic4"):
            st.session_state.query = "SELECT * FROM PAYMENT_DB.PUBLIC.CUSTOMERS WHERE RISK_SCORE < 50 OR TOTAL_TRANSACTIONS > 20"
    
    # Locked sections
    locked_sections = [
        ("Sorting & Limiting Results", "sorting", [
            ("ORDER BY AMOUNT DESC LIMIT 10", "SELECT * FROM PAYMENT_DB.PUBLIC.TRANSACTIONS ORDER BY AMOUNT DESC LIMIT 10"),
            ("DISTINCT COUNTRY", "SELECT DISTINCT COUNTRY FROM PAYMENT_DB.PUBLIC.MERCHANTS ORDER BY COUNTRY")
        ]),
        ("Aggregation & Grouping", "aggregation", [
            ("COUNT(*) transactions", "SELECT COUNT(*) FROM PAYMENT_DB.PUBLIC.TRANSACTIONS"),
            ("GROUP BY STATUS", "SELECT STATUS, COUNT(*) FROM PAYMENT_DB.PUBLIC.TRANSACTIONS GROUP BY STATUS")
        ]),
        ("Snowflake Date Functions", "snowflake", [
            ("DATE_TRUNC example", "SELECT * FROM PAYMENT_DB.PUBLIC.TRANSACTIONS WHERE DATE_TRUNC('MONTH', TRANSACTION_TIMESTAMP) = '2024-01-01'"),
            ("TRY_CAST example", "SELECT TRY_CAST(AMOUNT AS NUMBER(10,2)) AS FORMATTED_AMOUNT FROM PAYMENT_DB.PUBLIC.TRANSACTIONS WHERE AMOUNT > 50")
        ]),
        ("JOIN Operations", "join", [
            ("INNER JOIN", "SELECT t.TRANSACTION_ID, t.AMOUNT, m.MERCHANT_NAME FROM PAYMENT_DB.PUBLIC.TRANSACTIONS t INNER JOIN PAYMENT_DB.PUBLIC.MERCHANTS m ON t.MERCHANT_ID = m.MERCHANT_ID LIMIT 10"),
            ("3-table JOIN", "SELECT t.TRANSACTION_ID, m.MERCHANT_NAME, c.EMAIL, t.AMOUNT FROM PAYMENT_DB.PUBLIC.TRANSACTIONS t JOIN PAYMENT_DB.PUBLIC.MERCHANTS m ON t.MERCHANT_ID = m.MERCHANT_ID JOIN PAYMENT_DB.PUBLIC.CUSTOMERS c ON t.CUSTOMER_ID = c.CUSTOMER_ID WHERE t.STATUS = 'SUCCESS' LIMIT 10")
        ]),
        ("Advanced Features", "advanced", [
            ("VARIANT data", "SELECT TRANSACTION_ID, METADATA FROM PAYMENT_DB.PUBLIC.TRANSACTIONS WHERE METADATA IS NOT NULL LIMIT 5"),
            ("Complex aggregation", "SELECT DATE_TRUNC('MONTH', TRANSACTION_TIMESTAMP) AS MONTH, COUNT(*) FROM PAYMENT_DB.PUBLIC.TRANSACTIONS GROUP BY DATE_TRUNC('MONTH', TRANSACTION_TIMESTAMP)")
        ])
    ]
    
    for section_name, section_key, queries in locked_sections:
        st.subheader(f"🔒 {section_name} (Password Protected)")
        
        if section_key not in st.session_state.unlocked_sections:
            password = st.text_input(f"Enter password for {section_name}:", type="password", key=f"pwd_{section_key}")
            if st.button(f"Unlock {section_name}", key=f"unlock_{section_key}"):
                if password == "ilovejj":
                    st.session_state.unlocked_sections.add(section_key)
                    st.success(f"🔓 {section_name} unlocked!")
                    st.rerun()
                else:
                    st.error("Incorrect password!")
        else:
            st.success(f"🔓 {section_name} (Unlocked)")
            col1, col2 = st.columns(2)
            for i, (desc, query) in enumerate(queries):
                col = col1 if i % 2 == 0 else col2
                with col:
                    if st.button(desc, key=f"{section_key}_{i}"):
                        st.session_state.query = query

# SQL Query Input
st.header("✍️ Write Your Snowflake SQL Query")

# Initialize query in session state if not exists
if 'query' not in st.session_state:
    st.session_state.query = ""

query = st.text_area(
    "Enter your Snowflake SELECT query here:",
    value=st.session_state.query,
    height=120,
    placeholder="""Enter your Snowflake SELECT query here...
Example: SELECT * FROM PAYMENT_DB.PUBLIC.TRANSACTIONS WHERE AMOUNT > 100 AND STATUS = 'SUCCESS'

Try Snowflake functions like:
- DATE_TRUNC('MONTH', TRANSACTION_TIMESTAMP)  
- TRY_CAST(AMOUNT AS NUMBER(10,2))
- CURRENT_TIMESTAMP()"""
)

# Execute button
if st.button("Execute Snowflake Query", type="primary"):
    if query.strip():
        if query.upper().strip().startswith('SELECT'):
            result = st.session_state.platform.execute_query(query)
            
            if result['success']:
                st.success(f"✅ Query executed successfully! Found {result['row_count']} rows.")
                
                if result.get('translated_query'):
                    st.info(f"🔄 Translated Snowflake syntax: `{result['translated_query']}`")
                
                if result['row_count'] > 0:
                    # Convert to DataFrame for better display
                    df = pd.DataFrame(result['data'], columns=result['columns'])
                    
                    # Display first 100 rows
                    display_df = df.head(100)
                    st.dataframe(display_df, use_container_width=True)
                    
                    if result['row_count'] > 100:
                        st.info(f"Showing first 100 rows of {result['row_count']} total results.")
                else:
                    st.info("No results found.")
            else:
                st.error(f"❌ Error: {result['error']}")
        else:
            st.error("Only SELECT queries are allowed for safety.")
    else:
        st.error("Please enter a query.")

# Footer
st.markdown("---")
st.markdown("Made with ❄️ for learning Snowflake SQL")