# Payment Gateway SQL Learning Platform

A Python-based platform for learning SQL queries using payment gateway transaction data.

## Features

- **3 Related Tables**: transactions, merchants, customers
- **Sample Data**: 1000 transactions, 50 merchants, 200 customers  
- **Safe Queries**: Only SELECT statements allowed
- **Web Interface**: Easy-to-use query interface
- **Schema View**: See all table structures

## Tables

1. **transactions** - Payment transaction records
2. **merchants** - Business account information  
3. **customers** - Customer account details

## Setup

```bash
pip install -r requirements.txt
python payment_gateway_platform.py
```

Open http://localhost:5000

## Example Queries

```sql
SELECT * FROM transactions LIMIT 10
SELECT * FROM transactions WHERE status = 'SUCCESS' AND amount > 100
SELECT merchant_name, business_type FROM merchants WHERE country = 'USA'
SELECT * FROM customers WHERE risk_score < 30 AND total_transactions > 10
```