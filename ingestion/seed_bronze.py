"""
Create bronze layer tables and populates them with synthetic data.
Schemas are defined here and tables are created on first run. 
Safe to re-run. 

Run from the project root:
    python ingestion/seed_bronze.py

"""

from datetime import datetime, timezone
import random
import uuid

import pyarrow as pa
from faker import Faker
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestamptzType
)

from config.catalog import CATALOG_PATH, WAREHOUSE_PATH, get_catalog


NUM_CUST = 200
NUM_MORTGAGE = 150
NUM_LOANS = 300

random.seed(42)
fake = Faker()
fake.seed_instance(42)

# Define Schema
CUSTOMER_SCHEMA = Schema(
    NestedField(1,  "customer_id",   StringType(),   required=False),
    NestedField(2,  "first_name",    StringType()),
    NestedField(3,  "last_name",     StringType()),
    NestedField(4,  "email",         StringType()),
    NestedField(5,  "date_of_birth", DateType()),
    NestedField(6,  "address",       StringType()),
    NestedField(7,  "city",          StringType()),
    NestedField(8,  "state",         StringType()),
    NestedField(9,  "zip_code",      StringType()),
    NestedField(10, "created_at",    TimestamptzType()),
)

MORTGAGE_SCHEMA = Schema(
    NestedField(1,  "mortgage_id",     StringType(),  required=False),
    NestedField(2,  "customer_id",     StringType(),  required=False),
    NestedField(3,  "property_value",  DoubleType()),
    NestedField(4,  "loan_amount",     DoubleType()),
    NestedField(5,  "interest_rate",   DoubleType()),
    NestedField(6,  "term_years",      IntegerType()),
    NestedField(7,  "start_date",      DateType()),
    NestedField(8,  "status",          StringType()),
    NestedField(9,  "monthly_payment", DoubleType()),
    NestedField(10, "created_at",      TimestamptzType()),
)

LOAN_SCHEMA = Schema(
    NestedField(1,  "loan_id",         StringType(),  required=False),
    NestedField(2,  "customer_id",     StringType(),  required=False),
    NestedField(3,  "loan_type",       StringType()),
    NestedField(4,  "amount",          DoubleType()),
    NestedField(5,  "interest_rate",   DoubleType()),
    NestedField(6,  "term_months",     IntegerType()),
    NestedField(7,  "issue_date",      DateType()),
    NestedField(8,  "status",          StringType()),
    NestedField(9,  "monthly_payment", DoubleType()),
    NestedField(10, "created_at",      TimestamptzType()),
)


# Helper 
def monthly_payment(principal: float, annual_rate: float, n_payments: int) -> float:
    r = annual_rate / 100 / 12
    if r == 0:
        return round(principal / n_payments, 2)
    return round(principal * (r * (1 + r) ** n_payments) / ((1 + r) ** n_payments - 1), 2)


# Data Generator
def generate_customers(n: int) -> pa.Table:
    now = datetime.now(timezone.utc)
    rows = [
        {
            "customer_id":   str(uuid.uuid4()),
            "first_name":    fake.first_name(),
            "last_name":     fake.last_name(),
            "email":         fake.email(),
            "date_of_birth": fake.date_of_birth(minimum_age=25, maximum_age=70),
            "address":       fake.street_address(),
            "city":          fake.city(),
            "state":         fake.state_abbr(),
            "zip_code":      fake.zipcode(),
            "created_at":    now,
        }
        for _ in range(n)
    ]
    return pa.Table.from_pylist(rows, schema=pa.schema([
        pa.field("customer_id",   pa.string()),
        pa.field("first_name",    pa.string()),
        pa.field("last_name",     pa.string()),
        pa.field("email",         pa.string()),
        pa.field("date_of_birth", pa.date32()),
        pa.field("address",       pa.string()),
        pa.field("city",          pa.string()),
        pa.field("state",         pa.string()),
        pa.field("zip_code",      pa.string()),
        pa.field("created_at",    pa.timestamp("us", tz="UTC")),
    ]))


def generate_mortgages(customer_ids: list, n: int) -> pa.Table:
    now      = datetime.now(timezone.utc)
    statuses = ["active", "active", "active", "closed", "defaulted"]
    terms    = [15, 20, 25, 30]
    rows = []
    for _ in range(n):
        property_value = round(random.uniform(150_000, 900_000), 2)
        loan_amount    = round(property_value * random.uniform(0.6, 0.95), 2)
        rate           = round(random.uniform(2.5, 7.5), 3)
        term_years     = random.choice(terms)
        rows.append({
            "mortgage_id":     str(uuid.uuid4()),
            "customer_id":     random.choice(customer_ids),
            "property_value":  property_value,
            "loan_amount":     loan_amount,
            "interest_rate":   rate,
            "term_years":      term_years,
            "start_date":      fake.date_between(start_date="-10y", end_date="today"),
            "status":          random.choice(statuses),
            "monthly_payment": monthly_payment(loan_amount, rate, term_years * 12),
            "created_at":      now,
        })
    return pa.Table.from_pylist(rows, schema=pa.schema([
        pa.field("mortgage_id",     pa.string()),
        pa.field("customer_id",     pa.string()),
        pa.field("property_value",  pa.float64()),
        pa.field("loan_amount",     pa.float64()),
        pa.field("interest_rate",   pa.float64()),
        pa.field("term_years",      pa.int32()),
        pa.field("start_date",      pa.date32()),
        pa.field("status",          pa.string()),
        pa.field("monthly_payment", pa.float64()),
        pa.field("created_at",      pa.timestamp("us", tz="UTC")),
    ]))


def generate_loans(customer_ids: list, n: int) -> pa.Table:
    now           = datetime.now(timezone.utc)
    loan_types    = ["personal", "auto", "student"]
    statuses      = ["active", "active", "closed", "defaulted"]
    amount_ranges = {"personal": (1_000, 50_000), "auto": (5_000, 80_000), "student": (10_000, 100_000)}
    terms         = [12, 24, 36, 48, 60, 84]
    rows = []
    for _ in range(n):
        loan_type = random.choice(loan_types)
        amount    = round(random.uniform(*amount_ranges[loan_type]), 2)
        rate      = round(random.uniform(3.0, 18.0), 3)
        term      = random.choice(terms)
        rows.append({
            "loan_id":         str(uuid.uuid4()),
            "customer_id":     random.choice(customer_ids),
            "loan_type":       loan_type,
            "amount":          amount,
            "interest_rate":   rate,
            "term_months":     term,
            "issue_date":      fake.date_between(start_date="-5y", end_date="today"),
            "status":          random.choice(statuses),
            "monthly_payment": monthly_payment(amount, rate, term),
            "created_at":      now,
        })
    return pa.Table.from_pylist(rows, schema=pa.schema([
        pa.field("loan_id",         pa.string()),
        pa.field("customer_id",     pa.string()),
        pa.field("loan_type",       pa.string()),
        pa.field("amount",          pa.float64()),
        pa.field("interest_rate",   pa.float64()),
        pa.field("term_months",     pa.int32()),
        pa.field("issue_date",      pa.date32()),
        pa.field("status",          pa.string()),
        pa.field("monthly_payment", pa.float64()),
        pa.field("created_at",      pa.timestamp("us", tz="UTC")),
    ]))


def main():
    print("Seeding bronze layer...")
    catalog = get_catalog()

    # Create table if doesn't exists
    # Create tables if they don't exist
    for identifier, schema in [
        (("bronze", "customers"), CUSTOMER_SCHEMA),
        (("bronze", "mortgages"), MORTGAGE_SCHEMA),
        (("bronze", "loans"),     LOAN_SCHEMA),
    ]:
        if not catalog.table_exists(identifier):
            catalog.create_table(identifier, schema=schema)
            print(f"  Created table: {'.'.join(identifier)}")
        else:
            print(f"  Table already exists: {'.'.join(identifier)}")

    # Generate and write data
    print("\nWriting data...")

    customers_arrow = generate_customers(NUM_CUST)
    customer_ids    = customers_arrow.column("customer_id").to_pylist()
    catalog.load_table(("bronze", "customers")).append(customers_arrow)
    print(f"  Wrote {NUM_CUST} rows -> bronze.customers")

    mortgages_arrow = generate_mortgages(customer_ids, NUM_MORTGAGE)
    catalog.load_table(("bronze", "mortgages")).append(mortgages_arrow)
    print(f"  Wrote {NUM_MORTGAGE} rows -> bronze.mortgages")

    loans_arrow = generate_loans(customer_ids, NUM_LOANS)
    catalog.load_table(("bronze", "loans")).append(loans_arrow)
    print(f"  Wrote {NUM_LOANS} rows -> bronze.loans")

    print("\nBronze layer seeded successfully.")


if __name__ == "__main__":
    main()