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
            return [dict(row) for row in result]
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

# Process user query
def process_query(user_query):
    source = route_query(user_query)
    if source == "rds":
        sql_query = f"""
        SELECT * FROM products 
        WHERE title ILIKE '%{user_query}%' 
        OR main_category ILIKE '%{user_query}%';
        """
        results = query_rds(sql_query)
        return {"source": "RDS", "results": results}
    else:
        results = query_pinecone(user_query)
        return {"source": "Pinecone", "results": results}

# Recommend similar products
def recommend_products_by_title(product_title):
    try:
        sql_query = f"""
        SELECT * FROM products 
        WHERE title ILIKE '%{product_title}%';
        """
        product = query_rds(sql_query)
        if not product:
            st.warning(f"No product found for title: {product_title}")
            return []

        product_id = product[0].get("parent_asin")
        main_category = product[0].get("main_category")
        
        recommendations_query = f"""
        SELECT p.title, p.main_category, 
               AVG(r.rating) AS avg_rating, 
               COUNT(r.rating) AS rating_count 
        FROM products p
        INNER JOIN reviews3 r ON p.parent_asin = r.parent_asin
        WHERE p.parent_asin != '{product_id}'
          AND p.main_category = '{main_category}'
        GROUP BY p.parent_asin, p.title, p.main_category
        HAVING COUNT(r.rating) > 0
        ORDER BY avg_rating DESC, rating_count DESC
        LIMIT 5;
        """
        recommendations = query_rds(recommendations_query)
        return recommendations
    except Exception as e:
        st.error(f"Error fetching recommendations: {e}")
        return []

# Streamlit Interface
st.title("AMGR")
st.write("Ask questions about products, or get recommendations!")

# Search Input
user_query = st.text_input("Enter your query")
if st.button("Search"):
    if user_query:
        with st.spinner("Searching..."):
            response = process_query(user_query)
        st.write(f"Search Source: {response['source']}")
        if response['source'] == "RDS":
            for result in response["results"]:
                st.write(result)
        else:
            for match in response["results"]:
                metadata = match["metadata"]
                score = match["score"]
                st.write(f"**Title**: {metadata.get('title', 'N/A')}, **Category**: {metadata.get('main_category', 'N/A')}, **Score**: {score}")

# Recommendations Input
product_title = st.text_input("Enter Product Title for Recommendations")
if st.button("Recommend Similar Products"):
    if product_title:
        with st.spinner("Fetching recommendations..."):
            recommendations = recommend_products_by_title(product_title)
        if recommendations:
            st.write("Top-Rated Similar Products:")
            for rec in recommendations:
                st.write(
                    f"**Title**: {rec.get('title', 'N/A')}, "
                    f"**Category**: {rec.get('main_category', 'N/A')}, "
                    f"**Average Rating**: {rec.get('avg_rating', 'N/A'):.2f}, "
                    f"**Number of Ratings**: {rec.get('rating_count', 0)}"
                )
        else:
            st.warning("No recommendations found.")
