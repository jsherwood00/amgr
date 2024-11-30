from create import create_db, print_rows, insert_json

if __name__ == "__main__":
    create_db()
    print_rows("products")
    print_rows("user_reviews")
    
    test_json_file = 'All_Beauty.jsonl'
    insert_json(test_json_file)