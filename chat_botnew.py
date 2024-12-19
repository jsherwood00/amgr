# Import libraries
import streamlit as st
import openai
from pinecone import Pinecone, ServerlessSpec
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import base64
import os

# API Keys and Database Connection
openai.api_key = "sk-proj-FIcwlA65aFh7wow55kcuT3BlbkFJDADHefllVsohIzjx1CUx"
PINECONE_API_KEY = "pcsk_79d2Yp_3rZFLCJmzwm29wjJ2qMZAtUk41eLLfANmo8vtCLNN6amC4gKDuToaD5GH2VifmY"
PINECONE_ENVIRONMENT = "us-east-1"
DATABASE_URI = "postgresql+psycopg2://postgres:ds87398HFAERbbbvyufindsdfghyui@chatbot.cifjclcixjjs.us-east-2.rds.amazonaws.com/postgres"

# Initialize Pinecone
pinecone_client = Pinecone(api_key=PINECONE_API_KEY)

# Check or create index
index_name = "amgr"
if index_name not in pinecone_client.list_indexes().names():
    pinecone_client.create_index(
        name=index_name,
        dimension=1536,  # Adjust to your embedding size
        metric="cosine",
        spec=ServerlessSpec(cloud="aws", region=PINECONE_ENVIRONMENT)
    )
index = pinecone_client.Index(index_name)

# Connect to RDS
engine = create_engine(DATABASE_URI)

# Route query to RDS or Pinecone
def route_query(query):
    if "price" in query.lower() or "category" in query.lower():
        return "rds"
    return "pinecone"

# Query RDS
def query_rds(sql_query):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql_query))
            # Convert result rows to dictionaries manually
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result]
    except Exception as e:
        st.error(f"Error querying RDS: {e}")
        return []

# Query Pinecone
def query_pinecone(query):
    try:
        embedding = openai.Embedding.create(input=query, model="text-embedding-ada-002")
        vector = embedding["data"][0]["embedding"]
        result = index.query(vector=vector, top_k=5, include_metadata=True)
        return result["matches"]
    except Exception as e:
        st.error(f"Error querying Pinecone: {e}")
        return []

# Fetch details from RDS using parent_asin
def fetch_details_from_rds(parent_asin):
    try:
        sql_query = f"""
        SELECT description, features
        FROM products
        WHERE parent_asin = '{parent_asin}';
        """
        result = query_rds(sql_query)
        if result:
            return result[0]  # Return the first matching record
        else:
            return {"description": "No description available", "features": "No features available"}
    except Exception as e:
        st.error(f"Error fetching details from RDS for {parent_asin}: {e}")
        return {"description": "No description available", "features": "No features available"}

# Process user query
def process_query(user_query):
    source = route_query(user_query)
    matches = query_pinecone(user_query)
    enriched_results = []

    for match in matches:
        parent_asin = match.get("id")  # Pinecone uses `id` as `parent_asin`
        metadata = match.get("metadata", {})

        # Fetch additional details from RDS
        details = fetch_details_from_rds(parent_asin)

        enriched_result = {
            "parent_asin": parent_asin,
            "title": metadata.get("title", "N/A"),
            "main_category": metadata.get("main_category", "N/A"),
            "description": details.get("description", "No description available"),
            "features": details.get("features", "No features available"),
        }
        enriched_results.append(enriched_result)

    return {"source": "Pinecone", "results": enriched_results}

# Streamlit Interface
st.title("Amazon Product Chatbot")
st.write("Ask questions about products, or get recommendations!")

# Search Input
user_query = st.text_input("Enter your query")
if st.button("Search"):
    if user_query:
        with st.spinner("Searching..."):
            response = process_query(user_query)
        for result in response["results"]:
            title = result['title'][:1000]
            main_category = result['main_category'][:1000]
            description = result['description'][:1000]
            features = result['features'][:1000]

            st.markdown(f"### {title}")
            st.write(f"**Category:** {main_category}")
            st.write(f"**Description:** {description}")
            st.write(f"**Features:** {features}")
            st.markdown("---")
