import psycopg2
from psycopg2 import sql, extras
import json
import boto3
import os

# AWS RDS credentials
host = 'chatbot.cifjclcixjjs.us-east-2.rds.amazonaws.com'
database = 'postgres'
user = 'postgres'
password = 'ds87398HFAERbbbvyufindsdfghyui'
port = "5432"

ROWS_PER_BATCH = 1000


def get_conn():
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )
    return conn


def close_cursor(cursor):
    if cursor is not None:
        cursor.close()


def close_conn(conn):
    if conn is not None:
        conn.close()
    
        
def exec_query(query):
    try:
        conn = get_conn()
        cursor = conn.cursor()
        
        cursor.execute(query)
        conn.commit()

    except Exception as error:
        print(f"Error: {error}")

    finally:
        # Close the cursor and connection if they were created
        close_cursor(cursor)
        close_conn(conn)

def rows_on_query(query):
    conn = get_conn()
    cursor = conn.cursor()
    
    cursor.execute(query)
    conn.commit()
    
    rows = cursor.fetchall()
    close_cursor(cursor)
    close_conn(conn)

    return rows

def size_tables():
    user_query = "SELECT COUNT(*) FROM Reviews2;"
    product_query = "SELECT COUNT(*) FROM Products2;"

    reviews_query = "SELECT COUNT(*) FROM Reviews"
    product1_query = "SELECT COUNT(*) FROM Products"

    user_rows = (rows_on_query(user_query))[0][0]
    product_rows = (rows_on_query(product_query))[0][0]
    #reviews = (rows_on_query(reviews_query))[0][0]
    #products = (rows_on_query(product1_query))[0][0]

    #print(f"Reviews1 is {reviews} and Products1 is {products} and Reviews2 is {user_rows} and Products2 is {product_rows}\n")
    
    return user_rows, product_rows


# Currently checks duplicates for user reviews
def check_duplicates():
    reviews_query = """
SELECT rid, COUNT(*)
FROM Reviews
GROUP BY rid
HAVING COUNT(*) > 1;
"""

    products_query = """
SELECT parent_asin, COUNT(*)
FROM Products
GROUP BY parent_asin
HAVING COUNT(*) > 1;
"""

    products_rows = rows_on_query(reviews_query)
    reviews_rows = rows_on_query(products_query)
    print(rows)

# destroys new table (product2)
def destroy_new():
    query = "DROP TABLE IF EXISTS Reviews2"
    exec_query(query)

# WARNING: DESTROYS ENTIRE DATABASE
def destroy_db():
    query = "DROP TABLE IF EXISTS Details"
    query2 = "DROP TABLE IF EXISTS Products"
    exec_query(query)
    exec_query(query2)
    
    check_table_query = """
    SELECT EXISTS (SELECT 1 FROM information_schema.tables 
WHERE table_name = 'Details')"""

    check_table_query_2 = """
    SELECT EXISTS (SELECT 1 FROM information_schema.tables 
WHERE table_name = 'Products')"""
    
    result = rows_on_query(check_table_query)
    result2 = rows_on_query(check_table_query_2)
    
    if (not result or not result[0][0]) and (not result2 or not result2[0][0]):
        print("DATABASE DESTROYED")
    else:
        print("FAILED TO DESTROY DATABASE")



def insert_bundles_data(json_file):
    try:
        conn = get_conn()
        cursor = conn.cursor()

        # Load JSON data
        with open(json_file, 'r') as f:
            data = [json.loads(line) for line in f]

        # Insert all reviews into the table
        insert_query = """
        INSERT INTO Bundles (
            parent_asin_1,
            parent_asin_2) VALUES %s
            ON CONFLICT DO NOTHING
        """
        batch_size = 0
        iteration = 0
        batch = []
        
        for review in data:
            parent_1 = review.get('parent_asin', None)
            parent_2_list = review.get('bought_together', None)
            appended = 0
            if parent_2_list is not None:
                for parent_2 in parent_2_list:
                    batch.append(parent_1, parent_2)
                
            batch_size += appended
            if batch_size >= ROWS_PER_BATCH:
                try:
                    # multiple inserts on one query with %s placeholders
                    psycopg2.extras.execute_values(cursor, insert_query, batch)
                    
                    # Batch process: commit per ROWS_PER_BATCH rows
                    conn.commit()
                    
                    batch_size = 0
                    iteration += 1
                    # print(f"batch {iteration} completed!")
                    batch.clear() #clears the list for next set of reviews
        
                except Exception as e:
                    print(f"Insertion Error: {e} for file: " + json_file)
                    # Roll back the transaction if an error occurs
                    conn.rollback()
                    batch.clear() #clears the list for next set of reviews
                    return
    except Exception as e:
        print(f"Error: {e}")

    finally:
        close_cursor(cursor)
        close_conn(conn)

        # print("All rows inserted successfully!")    

# def insert_details_data(json_file):
#     try:
#         conn = get_conn()
#         cursor = conn.cursor()
        
#         # Load JSON data
#         with open(json_file, 'r') as f:
#             data = [json.loads(line) for line in f]
        
#         insert_query = """INSERT INTO Details 
#         (parent_asin,
#         first_date_available,
#         color, 
#         size, 
#         material,
#         brand, 
#         style, 
#         dimensions,
#         upc,
#         manufacturer) VALUES %sON CONFLICT DO NOTHING"""
                               
#         batch_size = 0
#         iteration = 0
#         batch = []
            
#         for product in data:
#             details = product.get('details', None)
#             if any(value is not None for value in details.values()):
#                 for value in details.values():
#                     print("found good match" + value + ", so now inserting it.\n")
#                 batch.append((
#                     product.get('parent_asin', None),
#                     details.get('Date First Available', None),
#                     details.get('color', None),
#                     details.get('size', None),
#                     details.get('material', None),
#                     details.get('brand', None),
#                     details.get('style', None),
#                     details.get('dimensions', None),
#                     details.get('upc', None),
#                     details.get('manufacturer', None)
#                 ))
            
#             if batch_size == ROWS_PER_BATCH or product == data[-1]:
#                 try:
#                     # multiple inserts on one query with %s placeholders
#                     psycopg2.extras.execute_values(cursor, insert_query, batch)
                    
#                     # Batch process: commit per ROWS_PER_BATCH rows
#                     conn.commit()
                    
#                     batch_size = 0
#                     iteration += 1
#                     print(f"batch {iteration} completed!")
#                     batch.clear() #clears the list for next set of reviews

#                 except Exception as e:
#                     print(f"Insertion Error: {e}")
#                     # Roll back the transaction if an error occurs
#                     conn.rollback()
#                     batch.clear() #clears the list for next set of reviews
#                     return

#         print("All rows inserted successfully!")
        
#     except Exception as e:
#         print(f"Error: {e}")

#     finally:
#         close_cursor(cursor)
#         close_conn(conn)


def insert_product_data(json_file):
    try:
        num_inserted = 0
        conn = get_conn()
        cursor = conn.cursor()
        
        # Load JSON data
        with open(json_file, 'r') as f:
            data = [json.loads(line) for line in f]
        
        insert_query = "INSERT INTO Products2 (parent_asin, main_category, title, rating_number,description, features, price, details) VALUES %sON CONFLICT DO NOTHING"                       
        batch_size = 0
        iteration = 0
        batch = []
        
            
        for product in data:
            details_dict = product.get('details', None)
            details_text = ''
            if details_dict is None or not isinstance(details_dict, (list, dict, str)):
                details_text = None
            else:
                for detail in details_dict:
                    details_text += detail
            
            price = product.get('price', None)
            if price is not None and type(price) != float:
                price = None
            
            category = product.get('main_category', None)
            title = product.get('title', None)
            description = product.get('description', None)
            features = product.get('features', None)

            # Sanitation: PostgreSQL sends an error if the string `0x00` is a substring of any field
            if (category is not None and '\x00' in category) or (title is not None and '\x00' in title) or (description is not None and '\x00' in description) or (features is not None and '\x00' in features) or '\x00'  in details_text :
                continue # skip this product

            batch.append((
                product.get('parent_asin', None),
                category,
                title,
                product.get('rating_number', None),
                description,
                features,
                price,
                details_text
            ))
            
            
            if batch_size == ROWS_PER_BATCH or product == data[-1]:
                try:
                    # multiple inserts on one query with %s placeholders
                    psycopg2.extras.execute_values(cursor, insert_query, batch)
                    
                    # Batch process: commit per ROWS_PER_BATCH rows
                    conn.commit()
                    
                    batch_size = 0
                    iteration += 1
                    # print(f"batch {iteration} completed!")
                    batch.clear() #clears the list for next set of reviews

                except Exception as e:
                    for batch_item in batch:
                        print(batch_item)
                    print(f"Insertion Error: {e}")
                    # Roll back the transaction if an error occurs
                    conn.rollback()
                    batch.clear() #clears the list for next set of reviews

        # print("All rows inserted successfully!")
        return num_inserted
        
    except Exception as e:
        print(f"Error: {e}")

    finally:
        close_cursor(cursor)
        close_conn(conn)

# insert user reviews
def insert_user_data(json_file):
    try:
        conn = get_conn()
        cursor = conn.cursor()

        # Load JSON data
        with open(json_file, 'r') as f:
            data = [json.loads(line) for line in f]

        # Insert all reviews into the table
        insert_query = """INSERT INTO 
            Reviews2 (parent_asin,
            text,
            title,
            rating)VALUES %s
            ON CONFLICT DO NOTHING"""
        batch_size = 0
        iteration = 0
        batch = []
        
        for review in data:

             # Sanitation: PostgreSQL sends an error if the string `0x00` is a substring of any field
            text = review.get('text', None)
            title = review.get('title', None)
            if (text is not None and '\x00' in text) or (title is not None and '\x00' in title):
                continue # skip this review
        
            batch.append((
                review.get('parent_asin', None),
                text,
                title,
                review.get('rating', None)
            ))
                
            batch_size += 1
            if batch_size == ROWS_PER_BATCH:
                try:
                    # multiple inserts on one query with %s placeholders
                    psycopg2.extras.execute_values(cursor, insert_query, batch)
                    
                    # Batch process: commit per ROWS_PER_BATCH rows
                    conn.commit()
                    
                    batch_size = 0
                    iteration += 1
                    # print(f"batch {iteration} completed!")
                    batch.clear() #clears the list for next set of reviews
        
                except Exception as e:
                    print(f"Insertion Error: {e}")
                    # Roll back the transaction if an error occurs
                    conn.rollback()
                    batch.clear() #clears the list for next set of reviews
                    return

        # print("All rows inserted successfully!")

    except Exception as e:
        print(f"Database connection failed: {e}")

    finally:
        close_cursor(cursor)
        close_conn(conn)
        print("Database connection closed.")


# Warning: this only works because all files currently on s3 are in jsonl format.
# TODO: Safety checks should be added in the future.
def load_from_s3():
    session = boto3.Session(
        aws_access_key_id="AKIARFZGTME2ZGGI5EXI",
        aws_secret_access_key="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",
        region_name="us-east-2"
    )

    s3 = session.resource('s3')
    bucket = s3.Bucket("amgr-mainbucket")

    for obj in bucket.objects.all():
        s3_key = obj.key
        local_file_path = os.path.join(os.getcwd(), os.path.basename(s3_key))
        bucket.download_file(obj.key, local_file_path)


# Prints all rows from the specified db
def print_rows(db_name):
    try:
        conn = get_conn()
        cursor = conn.cursor()
        query = f"SELECT * FROM {db_name}"
        cursor.execute(query)

        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        print(f"Columns: {column_names}")
        for row in rows:
            print(row)

    except Exception as e:
        print(f"Error: {e}")

    finally:
        close_cursor(cursor)
        close_conn(conn)


# Creates new products and review databases
def create_db():
    
    create_reviews_query = """
    CREATE TABLE IF NOT EXISTS Reviews (
        rid SERIAL, --PostgreSQL uses this to auto gen id
        parent_asin VARCHAR(20),
        text TEXT,
        title TEXT,
        rating FLOAT,
        PRIMARY KEY(rid)
    ); """

    create_reviews_query2 = """
    CREATE TABLE IF NOT EXISTS Reviews2 (
        rid SERIAL, --PostgreSQL uses this to auto gen id
        parent_asin VARCHAR(20),
        text TEXT,
        title TEXT,
        rating FLOAT,
        PRIMARY KEY(rid)
    ); """


    create_products_query = """
    CREATE TABLE IF NOT EXISTS Products (
        parent_asin VARCHAR(20),
        main_category VARCHAR(255),
        title TEXT,
        rating_number INT,
        description TEXT, 
        features TEXT, 
        price FLOAT,
        details TEXT,
        PRIMARY KEY (parent_asin)
    ) """

    create_products2_query = """
    CREATE TABLE IF NOT EXISTS Products2 (
        parent_asin VARCHAR(20),
        main_category VARCHAR(255),
        title TEXT,
        rating_number INT,
        description TEXT, 
        features TEXT, 
        price FLOAT,
        details TEXT,
        PRIMARY KEY (parent_asin)
    ) """
    
    # create_bundles_query = """
    # CREATE TABLE IF NOT EXISTS Bundles (
    #     parent_asin_1 VARCHAR(20),
    #     parent_asin_2 VARCHAR(20),
    #     PRIMARY KEY (parent_asin_1, parent_asin_2)
    # ) """
    
    # create_details_query = """
    # CREATE TABLE IF NOT EXISTS Details (
    #     parent_asin VARCHAR(20),
    #     first_date_available (date),
    #     color TEXT, -- store all except upc as TEXT for flexibility
    #     size TEXT,
    #     material TEXT,
    #     brand TEXT,
    #     style TEXT,
    #     dimensions TEXT,
    #     upc BIGINT,
    #     manufacturer TEXT,
    #     PRIMARY KEY (parent_asin)
    # ) """
     
    exec_query(create_reviews_query)
    exec_query(create_reviews_query2)
    exec_query(create_products_query)
    exec_query(create_products2_query)
    # exec_query(create_details_query)