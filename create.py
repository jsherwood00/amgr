import psycopg2
from psycopg2 import sql

# AWS RDS credentials
host = 'flowise-chatbot-database-instance-1.cifjclcixjjs.us-east-2.rds.amazonaws.com'
database = 'postgres'
user = 'postgres'
password = 'ds87398HFAERbbbvyufindsdfghyui'
port = "5432"


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


def insert_json(json_file):
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
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (asin, user_id, "timestamp") DO NOTHING;
        """  # Skip rows that violate the unique constraint

        for review in data:
            try:
                cursor.execute(insert_query, (
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
            except Exception as e:
                print(f"Error inserting the review: {review}. Error: {e}")
                conn.rollback()  # Roll back the transaction if an error occurs
            else:
                conn.commit()  # Commit after each successful insert

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
    
    create_reviews_table_query = """
    CREATE TABLE IF NOT EXISTS products (
        main_category VARCHAR(255),
        title VARCHAR(255),
        average_rating FLOAT CHECK (average_rating BETWEEN 0 AND 5),
        rating_number INT,
        features TEXT, -- Storing as JSON string for flexibility
        description TEXT, -- Storing as JSON string for flexibility
        price FLOAT,
        images TEXT, -- Storing as JSON string for flexibility
        videos TEXT, -- Storing as JSON string for flexibility
        store VARCHAR(255),
        categories TEXT, -- Storing as JSON string for flexibility
        details JSONB, -- Using JSONB for structured product details
        parent_asin VARCHAR(20),
        bought_together TEXT, -- Storing as JSON string for flexibility
        PRIMARY KEY (parent_asin, title)
    );
    """
    
    create_products_table_query = """
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
        bought_together,
    );
    """
        
    exec_query(create_reviews_table_query)
    exec_query(create_products_table_query)
