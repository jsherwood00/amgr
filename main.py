from create import print_rows, insert_user_data, insert_product_data, insert_bundles_data, size_tables, check_duplicates, destroy_db, create_db, load_from_s3, destroy_new
import os
from glob import glob
import boto3
import json

# Extracts appropriate fields from reviews jsonl file, uploading to `Reviews`
def insert_from_reviews(directory_path: str):
    file_pattern = os.path.join(directory_path, "*.jsonl") # only jsonl files
    jsonl_files = glob(file_pattern) # list of all .jsonl in dir
    size_dic = {
    "All_Beauty": 701528,
    "Amazon_Fashion": 2500939,
    "Appliances": 2128605,
    "Arts_Crafts_and_Sewing": 8966758,
    "Automotive": 19955450,
    "Baby_Products": 6028884,
    "Beauty_and_Personal_Care": 23911390,
    "Books": 29475453,
    "CDs_and_Vinyl": 4827273,
    "Cell_Phones_and_Accessories": 20812945,
    "Clothing_Shoes_and_Jewelry": 66033346,
    "Digital_Music": 130434,
    "Electronics": 43886944,
    "Gift_Cards": 152410,
    "Grocery_and_Gourmet_Food": 14318520,
    "Handmade_Products": 664162,
    "Health_and_Household": 25631345,
    "Health_and_Personal_Care": 494121,
    "Home_and_Kitchen": 67409944,
    "Industrial_and_Scientific": 5183005,
    "Kindle_Store": 25577616,
    "Magazine_Subscriptions": 71497,
    "Movies_and_TV": 17328314,
    "Musical_Instruments": 3017439,
    "Office_Products": 12845712,
    "Patio_Lawn_and_Garden": 16490047,
    "Pet_Supplies": 16827862,
    "Software": 4880181,
    "Sports_and_Outdoors": 19595170,
    "Subscription_Boxes": 16216,
    "Tools_and_Home_Improvement": 26982256,
    "Toys_and_Games": 16260406,
    "Unknown": 63814110,
    "Video_Games": 4624615
}
    
    for json_file in jsonl_files:
        if not os.path.basename(json_file).startswith('meta'): # only reviews
            before_insert_size = size_tables()
            print('Inserting user from' + json_file)
            insert_user_data(json_file)
            after_insert_size = size_tables()

            # load lines in json file (~how many rows should be uploaded)
            with open(json_file, 'r') as file:
                len_file = size_dic.get(json_file)
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
    
    # Reviews: 153M,105,000 / 571M
    # Products: / 48M
    
    # print_rows("Reviews")
    # size_tables()
    #a, b = size_tables()
    #print(f"Size reviews is {a} and size meta is {b}\n")
    # check_duplicates()
    
    #destroy_new()
    #create_db()     
                                         
                                         
                                         
                                               