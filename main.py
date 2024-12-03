from create import print_rows, insert_user_json_data, insert_meta_json_data, size_tables, check_duplicates, destroy_db, create_db
import glob
import os

def insert_dir_reviews(directory_path: str):
    file_pattern = os.path.join(directory_path, "*.jsonl") # only jsonl files
    jsonl_files = glob.glob(file_pattern) # list of all .jsonl in dir
    
    for json_file in jsonl_files:
        if not os.path.basename(json_file).startswith('meta'): # only reviews
            print(f"Inserting data from {json_file}...")
            insert_user_json_data(json_file)
            
def insert_dir_meta(directory_path: str):
    file_pattern = os.path.join(directory_path, "*.jsonl") # only jsonl files
    jsonl_files = glob.glob(file_pattern) # list of all .jsonl in dir
    
    for json_file in jsonl_files:
        # if os.path.basename(json_file).startswith('meta'): # only meta
        #     print(f"Inserting data from {json_file}...")
        #     insert_meta_json_data(json_file)
        # temp fix:
        print(f"Inserting data from {json_file}...")
        insert_meta_json_data(json_file)
        

if __name__ == "__main__":

    ### UNCOMMENT FOR ALL UPLOADING
    directory_path = os.getcwd()
    
    ### UNCOMMENT FOR UPLOADING REVIEWS
    # insert_dir_reviews(directory_path)
    
    ### UNCOMMENT FOR UPLOADING PRODUCT METADATA
    insert_dir_meta(directory_path)
    
    ### Other utility functions
    # print_rows("products")
    # print_rows("user_reviews")
    size_tables()
    # check_duplicates()
    
    
    ### Admin utility functions
    ### WARNING DO NOT USE `destroy_db()`` WITHOUT GROUP APPROVAL ###
    ### WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING ###
    # destroy_db()  
    # create_db()                                                
    ### WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING ###
