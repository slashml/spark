from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import requests
import json
from datetime import datetime, timedelta
import random
import logging
import argparse

class SparkSQLDemo:
    def __init__(self, api_endpoint="http://98.84.12.82:8000/v1/chat/completions"):
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Store API endpoint
        self.api_endpoint = api_endpoint
        
        print("Initializing Spark session...")
        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName("SQL Generation Demo") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        
        print("Creating sample data...")
        # Create sample data and schema
        self.create_sample_data()
        
        # Store the schema as a string for the model
        self.schema_str = """
        CREATE TABLE sales (
            transaction_id INTEGER,
            product_id INTEGER,
            customer_id INTEGER,
            sale_date DATE,
            quantity INTEGER,
            unit_price DOUBLE,
            total_amount DOUBLE
        );

        CREATE TABLE products (
            product_id INTEGER,
            product_name VARCHAR,
            category VARCHAR,
            supplier_id INTEGER
        );

        CREATE TABLE customers (
            customer_id INTEGER,
            customer_name VARCHAR,
            country VARCHAR,
            join_date DATE
        );
        """
        
        print("\nAvailable tables: sales, products, customers")
        print("\nSample data preview:")
        print("\nProducts:")
        self.products_df.show(3)
        print("\nCustomers:")
        self.customers_df.show(3)
        print("\nSales:")
        self.sales_df.show(3)

    def create_sample_data(self):
        # Define schemas
        sales_schema = StructType([
            StructField("transaction_id", IntegerType(), False),
            StructField("product_id", IntegerType(), False),
            StructField("customer_id", IntegerType(), False),
            StructField("sale_date", DateType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DoubleType(), False),
            StructField("total_amount", DoubleType(), False)
        ])

        products_schema = StructType([
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), False),
            StructField("category", StringType(), False),
            StructField("supplier_id", IntegerType(), False)
        ])

        customers_schema = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("customer_name", StringType(), False),
            StructField("country", StringType(), False),
            StructField("join_date", DateType(), False)
        ])

        # Generate sample data
        products_data = [
            (1, "Laptop", "Electronics", 101),
            (2, "Smartphone", "Electronics", 102),
            (3, "Desk Chair", "Furniture", 103),
            (4, "Coffee Maker", "Appliances", 101),
            (5, "Headphones", "Electronics", 102)
        ]

        customers_data = [
            (1, "John Smith", "USA", datetime(2023, 1, 15)),
            (2, "Maria Garcia", "Spain", datetime(2023, 2, 1)),
            (3, "David Kim", "South Korea", datetime(2023, 3, 10)),
            (4, "Emma Wilson", "UK", datetime(2023, 4, 5)),
            (5, "Lucas Silva", "Brazil", datetime(2023, 5, 20))
        ]

        # Generate random sales data
        sales_data = []
        for i in range(100):
            product_id = random.randint(1, 5)
            customer_id = random.randint(1, 5)
            sale_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 364))
            quantity = random.randint(1, 5)
            unit_price = random.uniform(100, 1000)
            total_amount = quantity * unit_price
            
            sales_data.append((
                i + 1,  # transaction_id
                product_id,
                customer_id,
                sale_date,
                quantity,
                unit_price,
                total_amount
            ))

        # Create DataFrames
        self.sales_df = self.spark.createDataFrame(sales_data, sales_schema)
        self.products_df = self.spark.createDataFrame(products_data, products_schema)
        self.customers_df = self.spark.createDataFrame(customers_data, customers_schema)

        # Create temporary views
        self.sales_df.createOrReplaceTempView("sales")
        self.products_df.createOrReplaceTempView("products")
        self.customers_df.createOrReplaceTempView("customers")

    def test_api_connection(self):
        """Test the API connection before starting the demo"""
        try:
            response = requests.get(self.api_endpoint.replace("/v1/chat/completions", "/health"))
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False

    def generate_sql_query(self, question):
        if not self.test_api_connection():
            return "Error: Cannot connect to the SQL generation API. Please check your internet connection and API endpoint."

        headers = {
            "Content-Type": "application/json"
        }

        base_prompt = f"""### Task
Generate a SQL query to answer [QUESTION]{question}[/QUESTION]

### Instructions
- If you cannot answer the question with the available database schema, return 'I do not know'

### Database Schema
{self.schema_str}

### Answer
Given the database schema, here is the SQL query that answers [QUESTION]{question}[/QUESTION]
[SQL]
"""

        data = {
            "model": "slashml/text-to-sql",
            "messages": [
                {"role": "user", "content": base_prompt}
            ]
        }

        try:
            response = requests.post(
                self.api_endpoint,
                headers=headers,
                json=data,
                timeout=10
            )
            response.raise_for_status()
            return response.json()['choices'][0]['message']['content']
        except requests.exceptions.Timeout:
            return "Error: API request timed out. Please try again."
        except requests.exceptions.RequestException as e:
            return f"Error connecting to API: {str(e)}"
        except (KeyError, json.JSONDecodeError) as e:
            return f"Error parsing API response: {str(e)}"
        except Exception as e:
            return f"Unexpected error: {str(e)}"

    def execute_query(self, sql_query):
        try:
            result = self.spark.sql(sql_query)
            return result
        except Exception as e:
            return f"Error executing SQL query: {str(e)}"

    def ask_question(self, question):
        print(f"\nQuestion: {question}")
        print("-" * 50)
        
        # Generate SQL query
        sql_query = self.generate_sql_query(question)
        print(f"Generated SQL Query:")
        print(sql_query)
        print("-" * 50)
        
        # Execute query if we got a valid SQL query
        if not sql_query.startswith("Error"):
            result = self.execute_query(sql_query)
            print("Query Result:")
            if isinstance(result, str):
                print(result)
            else:
                result.show()
        else:
            print("Could not execute query due to generation error")
        
        print("=" * 50)

    def show_help(self):
        """Display help information"""
        help_text = """
Available Commands:
-----------------
help    : Show this help message
schema  : Show the database schema
preview : Show sample data preview
exit    : Exit the program

Example Questions:
----------------
- What are the total sales for each product category?
- Who are the top 5 customers by total purchase amount?
- What is the average order value by country?
- How many products were sold in each month of 2023?
- Which products have never been sold?

Note: You can ask any question about the sales, products, and customers tables.
        """
        print(help_text)

    def interactive_mode(self):
        """Run the demo in interactive mode"""
        print("\nEntering interactive mode. Type 'exit' to quit, 'help' for commands, or enter your question.")
        
        while True:
            try:
                user_input = input("\nEnter your question > ").strip()
                
                if user_input.lower() == 'exit':
                    print("Exiting interactive mode...")
                    break
                elif user_input.lower() == 'help':
                    self.show_help()
                elif user_input.lower() == 'schema':
                    print("\nDatabase Schema:")
                    print(self.schema_str)
                elif user_input.lower() == 'preview':
                    print("\nSample data preview:")
                    print("\nProducts:")
                    self.products_df.show(3)
                    print("\nCustomers:")
                    self.customers_df.show(3)
                    print("\nSales:")
                    self.sales_df.show(3)
                elif user_input.strip():
                    self.ask_question(user_input)
                
            except KeyboardInterrupt:
                print("\nExiting interactive mode...")
                break
            except Exception as e:
                print(f"Error: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Interactive Spark SQL Demo')
    parser.add_argument('--api-endpoint', type=str, 
                      default="http://98.84.12.82:8000/v1/chat/completions",
                      help='API endpoint for SQL generation')
    args = parser.parse_args()

    # Initialize the demo
    demo = SparkSQLDemo(api_endpoint=args.api_endpoint)
    
    # Test API connection before starting
    if not demo.test_api_connection():
        print("Error: Cannot connect to the SQL generation API. Please check your internet connection and API endpoint.")
        return

    # Run in interactive mode
    demo.interactive_mode()
    
    # Clean up
    demo.spark.stop()

if __name__ == "__main__":
    main()
