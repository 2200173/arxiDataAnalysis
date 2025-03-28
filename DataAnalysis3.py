import pandas as panda
import sqlite3
import json

# URLs for the JSON files
urls = {
    'categories': 'https://raw.githubusercontent.com/tiagosantosarxi/data_analysis/main/categories.json',
    'products': 'https://raw.githubusercontent.com/tiagosantosarxi/data_analysis/main/products.json',
    'sales': 'https://raw.githubusercontent.com/tiagosantosarxi/data_analysis/main/sale_order_lines.json',
    'customers': 'https://raw.githubusercontent.com/tiagosantosarxi/data_analysis/main/contacts.json'
}

# Read each JSON file and create a DataFrame
dataframes = {}

for name, url in urls.items():
    try:
        dataframes[name] = panda.read_json(url)
        #print(f"DataFrame '{name}' created successfully.")
    except Exception as e:
        print(f"Error reading '{name}': {e}")

# SQLite database connection
conn = sqlite3.connect('data.db')

# Column mappings for the DataFrames
column_mappings = {
    'products': {'categ_id': ['categ_id_num', 'categ_id_name']},
    'customers': {'country_id': ['country_id_num', 'country_name']},
    'sales': {
        'order_id': ['order_id_num', 'order_id_name'],
        'product_id': ['product_id_num', 'product_id_name'],
        'order_partner_id': ['order_partner_id_num', 'order_partner_id_name']
    }
}

# Write each DataFrame to a table in the database
for name, df in dataframes.items():
    if name in column_mappings:
        for col, new_cols in column_mappings[name].items():
            df[new_cols[0]] = df[col].apply(lambda x: x[0] if isinstance(x, list) else None)
            df[new_cols[1]] = df[col].apply(lambda x: x[1] if isinstance(x, list) and len(x) > 1 else None)
            df.drop(col, axis=1, inplace=True)

    for col in df.columns:
        if any(isinstance(item, list) for item in df[col].dropna()):
            df[col] = df[col].apply(lambda x: ','.join(map(str, x)) if isinstance(x, list) else str(x))
        if any(isinstance(item, dict) for item in df[col].dropna()):
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else str(x))

    df.to_sql(name, conn, if_exists='replace', index=False)

# Function to execute queries
def execute_query(conn, query, query_name):
    cursor = conn.cursor()
    print(f"\n{query_name}:")
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        for row in results:
            print(row)
    except sqlite3.OperationalError as e:
        print(f"Error in query {query_name}: {e}")
    return results

# SQL Queries
query1 = """
SELECT 
    category, 
    product, 
    total_quantity_sold
FROM (
    SELECT 
        c.name AS category,
        p.name AS product,
        SUM(s.product_uom_qty) AS total_quantity_sold,
        RANK() OVER (PARTITION BY c.name ORDER BY SUM(s.product_uom_qty) DESC) AS ranking
    FROM 
        sales s
    LEFT JOIN 
        products p ON s.product_id_num = p.id
    LEFT JOIN 
        categories c ON p.categ_id_num = c.id
    WHERE 
        strftime('%Y', s.create_date) = '2024'
    GROUP BY 
        c.name, p.name
) AS ranked_products
WHERE 
    ranking = 1
ORDER BY 
    total_quantity_sold DESC;
"""

query2 = """
SELECT
    country,
    product,
    total_quantity_sold
FROM (
    SELECT
        COALESCE(cust.country_name, 'Unknown') AS country,
        p.name AS product,
        SUM(s.product_uom_qty) AS total_quantity_sold,
        RANK() OVER (PARTITION BY COALESCE(cust.country_name, 'Unknown') ORDER BY SUM(s.product_uom_qty) DESC) AS ranking
    FROM
        sales s
    LEFT JOIN
        products p ON s.product_id_num = p.id
    LEFT JOIN
        customers cust ON s.order_partner_id_num = cust.id
    WHERE
        p.name IS NOT NULL  -- Exclude NULL products
    GROUP BY
        COALESCE(cust.country_name, 'Unknown'), p.name
) AS CountryProductSales
WHERE
    ranking = 1
ORDER BY
    total_quantity_sold DESC;
"""

query3 = """
SELECT
    cust.id AS customer_id,
    cust.name AS customer_name,
    COUNT(DISTINCT s.product_id_num) AS distinct_products_count
FROM
    sales s
INNER JOIN
    customers cust ON s.order_partner_id_num = cust.id
WHERE
    s.product_id_num IS NOT NULL
    AND cust.name IS NOT NULL
    AND cust.name != ''
GROUP BY
    cust.id, cust.name
ORDER BY
    distinct_products_count DESC
LIMIT 2;
"""

# Show queries results
r1 = execute_query(conn, query1, "1. Most sold product in 2024 by category")
r2 = execute_query(conn, query2, "2. Most sold product by country")
r3 = execute_query(conn, query3, "3. Customer who bought the most different products")

conn.close()
