from create import print_rows, insert_user_json_data, insert_meta_json_data, size_tables, check_duplicates, destroy_db, create_db
import glob
import os

def insert_jsonl_files_in_directory(directory_path: str):
    file_pattern = os.path.join(directory_path, "*.jsonl") # only jsonl files
    jsonl_files = glob.glob(file_pattern) # list of all .jsonl in dir
    
    for json_file in jsonl_files:
        if not os.path.basename(json_file).startswith('meta'): # only reviews
            print(f"Inserting data from {json_file}...")
            insert_user_json_data(json_file)
        

if __name__ == "__main__":
    # Existing utility functions:
    
    destroy_db()
    # create_db()
    # print_rows("products")
    # print_rows("user_reviews")
    # size_tables()
    # check_duplicates()
    # insert_meta_json_data
    # insert_user_json_data('filename')

    # Batch process all files in /
    # directory_path = os.getcwd()
    # insert_jsonl_files_in_directory(directory_path)
