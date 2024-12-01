import psycopg2
from psycopg2 import sql, extras
import json

# AWS RDS credentials
host = 'flowise-chatbot-database-instance-1.cifjclcixjjs.us-east-2.rds.amazonaws.com'
database = 'postgres'
user = 'postgres'
password = 'ds87398HFAERbbbvyufindsdfghyui'
port = "5432"

ROWS_PER_BATCH = 10000


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

# Currently prints user review table size
def size_tables() -> int:
    user_query = "SELECT COUNT(*) FROM user_reviews;"
    meta_query = "SELECT COUNT(*) FROM products;"

    user_rows = (rows_on_query(user_query))[0][0]
    meta_rows = (rows_on_query(meta_query))[0][0]
    
    print(f"The number of user_reviews stored on RDS is {user_rows}")
    print(f"The number of products stored on RDS is {meta_rows}")
    
    return user_rows, meta_rows


def check_duplicates():
    query = """
SELECT asin, user_id, timestamp, COUNT(*)
FROM user_reviews
GROUP BY asin, user_id, timestamp
HAVING COUNT(*) > 1;
"""

    rows = rows_on_query(query)
    print(rows)

# WARNING: DESTROYS ENTIRE DATABASE
def destroy_db():
    query = "DROP TABLE IF EXISTS user_reviews"
    exec_query(query)
    
    check_table_query = """
    SELECT EXISTS (SELECT 1 FROM information_schema.tables 
WHERE table_name = 'user_reviews')"""
    
    result = rows_on_query(check_table_query)
    
    if not result or not result[0][0]:
        print("DATABASE DESTROYED")
    else:
        print("FAILED TO DESTROY DATABASE")
    

# insert product data reviews
# TODO: remember that the images and videos columns are not there
def insert_meta_json_data(json_file):
    # for now empty
        
    insert_query = """
    INSERT INTO products (
        main_category,
        title,
        average_rating,
        rating_number,
        features , 
        description , 
        price ,
        store ,
        categories,
        details,
        parent_asin ,
        bought_together) VALUES %S
        ON CONFLICT (parent_asin) DO NOTHING;"""
        
    batch_size = 0
    iteration = 0
    batch = []
    
    # Load JSON data
    with open(json_file, 'r') as f:
        data = [json.loads(line) for line in f]
        
    for review in data:
        batch.append((
            review.get('main_category', None),
            review.get('title', None),
            review.get('average_rating', None),
            review.get('rating_number', None),
            review.get('features', None),
            review.get('description', None),
            review.get('price', None),
            review.get('store', None),
            review.get('categories', None),
            review.get('details', None),
            review.get('parent_asin', None),
            review.get('bought_together', None),
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
                print(f"batch {iteration} completed!")
                batch.clear() #clears the list for next set of reviews

            except Exception as e:
                print(f"Insertion Error: {e}")
                # Roll back the transaction if an error occurs
                conn.rollback()
                batch.clear() #clears the list for next set of reviews
                return

    print("All rows inserted successfully!")



# insert user reviews
def insert_user_json_data(json_file):
    try:
        conn = get_conn()
        cursor = conn.cursor()

        # Load JSON data
        with open(json_file, 'r') as f:
            data = [json.loads(line) for line in f]

        # Insert all reviews into the table
        insert_query = """
        INSERT INTO user_reviews (
            rating,
            title,
            text,
            asin,
            parent_asin,
            user_id,
            timestamp,
            verified_purchase,
            helpful_vote
        ) VALUES %s
        ON CONFLICT (asin, user_id, "timestamp") DO NOTHING;
        """  # Skip rows that violate the unique constraint

        batch_size = 0
        iteration = 0
        batch = []
        
        for review in data:
            batch.append((
                review.get('rating', None),
                review.get('title', None),
                review.get('text', None),
                review.get('asin', None),
                review.get('parent_asin', None),
                review.get('user_id', None),
                review.get('timestamp', None),
                review.get('verified_purchase', None),
                review.get('helpful_vote', None)
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
                    print(f"batch {iteration} completed!")
                    batch.clear() #clears the list for next set of reviews
        
                except Exception as e:
                    print(f"Insertion Error: {e}")
                    # Roll back the transaction if an error occurs
                    conn.rollback()
                    batch.clear() #clears the list for next set of reviews
                    return

        print("All rows inserted successfully!")

    except Exception as e:
        print(f"Database connection failed: {e}")

    finally:
        close_cursor(cursor)
        close_conn(conn)
        print("Database connection closed.")


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
    
    create_products_table_query = """
    CREATE TABLE IF NOT EXISTS products (
        main_category VARCHAR(255),
        title TEXT,
        average_rating FLOAT CHECK (average_rating BETWEEN 0 AND 5),
        rating_number INT,
        features TEXT, -- Storing as JSON string for flexibility
        description TEXT, -- Storing as JSON string for flexibility
        price FLOAT,
        images TEXT, -- Storing as JSON string for flexibility
        videos TEXT, -- Storing as JSON string for flexibility
        store TEXT,
        categories TEXT, -- Storing as JSON string for flexibility
        details JSONB, -- Using JSONB for structured product details
        parent_asin VARCHAR(20),
        bought_together TEXT, -- Storing as JSON string for flexibility
        PRIMARY KEY (parent_asin)
    ) """
    
    create_reviews_table_query = """
    CREATE TABLE IF NOT EXISTS user_reviews (
        rating FLOAT,
        title TEXT,
        text TEXT,
        asin VARCHAR(20),
        parent_asin VARCHAR(20),
        user_id VARCHAR(40),
        timestamp BIGINT,
        verified_purchase BOOLEAN,
        helpful_vote INTEGER,
        PRIMARY KEY(asin, user_id, timestamp)
    ); """
        
    exec_query(create_products_table_query)
    exec_query(create_reviews_table_query)
