from create import print_rows, insert_user_data, insert_product_data, insert_bundles_data, size_tables, check_duplicates, destroy_db, create_db
import os
from glob import glob

# Extracts appropriate fields from reviews jsonl file, uploading to `Reviews`
def insert_from_reviews(directory_path: str):
    file_pattern = os.path.join(directory_path, "*.jsonl") # only jsonl files
    jsonl_files = glob(file_pattern) # list of all .jsonl in dir
    
    for json_file in jsonl_files:
        if not os.path.basename(json_file).startswith('meta'): # only reviews
            print(f"Inserting data from {json_file}...")
            insert_user_data(json_file)
            
# Extracts appropriate fields from meta jsonl file, uploading to `Products``
# and `Bundles`` tables
def insert_from_meta(directory_path: str):
    file_pattern = os.path.join(directory_path, "*.jsonl") # only jsonl files
    jsonl_files = glob(file_pattern) # list of all .jsonl in dir
    
    for json_file in jsonl_files:
        if os.path.basename(json_file).startswith('meta'): # only meta
            print('Inserting meta from' + json_file)
            insert_product_data(json_file)
            insert_bundles_data(json_file)        

if __name__ == "__main__":

    #directory_path = os.getcwd()
    #insert_from_reviews(directory_path)
    #insert_from_meta(directory_path)
    
    # print_rows("Reviews")
    size_tables()
    check_duplicates()
    
    # destroy_db()  
    # create_db()     
                                               