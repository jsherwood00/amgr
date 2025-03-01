This is the RAG chatbot created with a 5-person team in Fall 2024. The ETL pipeline requires paid AWS services (S3, RDS) and the paid Pinecone service, so the chatbot will not be functional upon launching the Flowise interface until credentials are provided. main.py is an interface, which calls create.py functions to set up and execute the ETL pipeline.

<br>

To launch the Flowise interface after running the ETL pipeline to load your data:

1) Install Flowise: `npm install -g flowise`

2) Launch Flowise: `npx flowise start`

3) Open `localhost:3000`, click Agentflows on the sidebar -> Add new -> Settings -> Add Agent -> select the json file from this repository.

<br>

We provide 2 approaches. The distinction is how the data are stored in our vector database.

1) Approach 1: flat indexing: each product is stored individually.

2) Approach 2: chunked: one index contains multiple products.

<br>

Project team:
  
  *Joshua Sherwood*
  
  *Aleks Szymczak*
  
  *Pranav Parthasarathy*
  
  *Zephyr Flanigan*
  
  *Yashitha Pobbareddy*

<br>

Special thanks to our mentor, Professor Roozbeh Koochaksaraei.

