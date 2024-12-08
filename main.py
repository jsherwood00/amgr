from create import print_rows, insert_user_data, insert_product_data, insert_bundles_data, size_tables, check_duplicates, destroy_db, create_db, load_from_s3, destroy_new
import os
from glob import glob
import boto3
import json

# Extracts appropriate fields from reviews jsonl file, uploading to `Reviews`
def insert_from_reviews(directory_path: str):
    file_pattern = os.path.join(directory_path, "*.jsonl") # only jsonl files
    jsonl_files = glob(file_pattern) # list of all .jsonl in dir
    
    for json_file in jsonl_files:
        if not os.path.basename(json_file).startswith('meta'): # only reviews
            _, before_insert_size = size_tables()
            print('Inserting user from' + json_file)
            insert_user_data(json_file)
            _, after_insert_size = size_tables()

            # load lines in json file (~how many rows should be uploaded)
            with open(json_file, 'r') as file:
                len_file = sum(1 for line in file if line.strip())
                missing_values = len_file - (after_insert_size - before_insert_size)
                # num values that failed to commit to RDS
                print(f"[INFO] Missing values: {missing_values}\n")
            
# Extracts appropriate fields from meta jsonl file, uploading to `Products``
# and `Bundles`` tables
def insert_from_meta(directory_path: str):
    file_pattern = os.path.join(directory_path, "*.jsonl") # only jsonl files
    jsonl_files = glob(file_pattern) # list of all .jsonl in dir
    
    j = 0
    for json_file in jsonl_files:
        if os.path.basename(json_file).startswith('meta'): # only meta
            before_insert_size, _ = size_tables()
            print('Inserting meta from' + json_file)
            insert_product_data(json_file)
            after_insert_size, _ = size_tables()

            # load lines in json file (~how many rows should be uploaded)
            with open(json_file, 'r') as file:
                len_file = sum(1 for line in file if line.strip())
                missing_values = len_file - (after_insert_size - before_insert_size)
                # num values that failed to commit to RDS
                print(f"[INFO] Missing values: {missing_values}\n")
            

if __name__ == "__main__":

    #size_tables()
    directory_path = os.getcwd()
    # load_from_s3()
    ### Inserting reviews not work correctly
    insert_from_reviews(directory_path)
    ### Inserting products works correctly
    #insert_from_meta(directory_path)
    
    153,105,000
    
    # print_rows("Reviews")
    # size_tables()
    #a, b = size_tables()
    #print(f"Size reviews is {a} and size meta is {b}\n")
    # check_duplicates()
    
    #destroy_new()
    #create_db()     
                                         
                                         
                                         
                                               