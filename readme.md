# Chat with Your Data using Spark and SlashML Text-to-SQL

This project demonstrates how to integrate Apache Spark with SlashML's Text-to-SQL model to enable natural language queries on your data. The demo includes a sample retail database with sales, products, and customers data that you can query using plain English.

```
Enter your question > What are the total sales by category?

Generated SQL Query:
SELECT 
    p.category,
    SUM(s.total_amount) as total_sales
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.category
ORDER BY total_sales DESC;

Query Result:
+----------+------------------+
|  category|       total_sales|
+----------+------------------+
|Electronics|         567890.45|
| Furniture|         234567.89|
|Appliances|         123456.78|
+----------+------------------+
```


## 🌟 Features

- Natural language to SQL query conversion using SlashML's Text-to-SQL model
- Interactive command-line interface for querying data
- Built-in sample retail database with:
  - Sales transactions
  - Product catalog
  - Customer information
- Real-time query generation and execution with Spark
- Data preview capabilities
- Schema visualization
- Comprehensive error handling

## 🚀 Getting Started

### Setting Up SlashML Text-to-SQL Model

1. Visit [dashboard.slashml.com](https://dashboard.slashml.com)
2. Create an account or log in
3. Navigate to Models section
4. Deploy a Text-to-SQL model
5. Copy the API endpoint URL

### Prerequisites

```bash
Python 3.7+
Apache Spark
pip install pyspark requests
```

### Dataset Structure

The demo includes a sample retail database with three tables:

1. **Sales Table**
```sql
CREATE TABLE sales (
    transaction_id INTEGER,
    product_id INTEGER,
    customer_id INTEGER,
    sale_date DATE,
    quantity INTEGER,
    unit_price DOUBLE,
    total_amount DOUBLE
);
```

2. **Products Table**
```sql
CREATE TABLE products (
    product_id INTEGER,
    product_name VARCHAR,
    category VARCHAR,
    supplier_id INTEGER
);
```

3. **Customers Table**
```sql
CREATE TABLE customers (
    customer_id INTEGER,
    customer_name VARCHAR,
    country VARCHAR,
    join_date DATE
);
```



### Running the Demo

1. Clone this repository:
```bash
git clone https://github.com/yourusername/spark-text-to-sql-demo.git
cd spark-text-to-sql-demo
```

2. Run the demo with your SlashML API endpoint:
```bash
python spark_sql_demo.py --api-endpoint "YOUR_SLASHML_API_ENDPOINT"
```

## 💡 Usage

### Available Commands

```
help    : Display help information and example questions
schema  : Show the database schema
preview : Show sample data preview
exit    : Exit the program
```

### Example Questions

The demo understands complex analytical questions such as:
```
- What are the total sales for each product category?
- Who are the top 5 customers by total purchase amount?
- What is the average order value by country?
- How many products were sold in each month of 2023?
- Which products have never been sold?
```

### Interactive Session Example

```bash
$ python spark_sql_demo.py
Initializing Spark session...
Creating sample data...

Available tables: sales, products, customers

Sample data preview:
Products:
+---------+------------+----------+-----------+
|product_id|product_name| category|supplier_id|
+---------+------------+----------+-----------+
|        1|     Laptop|Electronics|       101|
|        2| Smartphone|Electronics|       102|
|        3| Desk Chair| Furniture|       103|
+---------+------------+----------+-----------+

Enter your question > What are the total sales by category?

Generated SQL Query:
SELECT 
    p.category,
    SUM(s.total_amount) as total_sales
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.category
ORDER BY total_sales DESC;

Query Result:
+----------+------------------+
|  category|       total_sales|
+----------+------------------+
|Electronics|         567890.45|
| Furniture|         234567.89|
|Appliances|         123456.78|
+----------+------------------+
```

## 🔧 Customizing the Demo

### Using Your Own Data

To use your own data, modify the `create_sample_data` method in the `SparkSQLDemo` class:

1. Define your schemas:
```python
your_schema = StructType([
    StructField("column_name", DataType(), nullable=False),
    # Add more fields...
])
```

2. Create your data:
```python
your_data = [
    (value1, value2, ...),
    # Add more records...
]
```

3. Create DataFrame and register view:
```python
your_df = self.spark.createDataFrame(your_data, your_schema)
your_df.createOrReplaceTempView("your_table")
```

4. Update the schema string in `__init__`:
```python
self.schema_str = """
CREATE TABLE your_table (
    column_name DATA_TYPE,
    # Add more columns...
);
"""
```

## 🔍 Troubleshooting

1. API Connection Issues:
```bash
Error: Cannot connect to the SQL generation API
Solution: Verify your API endpoint and internet connection
```

2. Spark Installation:
```bash
Error: Spark not found
Solution: Ensure Spark is properly installed and SPARK_HOME is set
```

3. Query Generation:
```bash
Error: Failed to generate SQL query
Solution: Verify question is related to available schema/tables
```

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- [SlashML](https://slashml.com) for the Text-to-SQL model
- [Apache Spark](https://spark.apache.org) for the data processing framework

## ❓ Support

- For code issues: Open an issue in this repository
- For SlashML questions: Visit [dashboard.slashml.com](https://dashboard.slashml.com)
- For Spark questions: Visit [spark.apache.org/docs](https://spark.apache.org/docs/latest/)
