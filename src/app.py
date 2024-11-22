from flask import Flask, request
from langchain_ollama import OllamaLLM
from langchain.chains import create_sql_query_chain
from langchain_community.utilities import SQLDatabase
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough, RunnableLambda
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool

from operator import itemgetter

from dotenv import load_dotenv
import os

app = Flask(__name__)

load_dotenv()

def recommendation(question):

    host = os.getenv('HOST')
    database = os.getenv('DB_NAME')
    user = os.getenv('USER')
    password = os.getenv('DB_PASSWORD')

    llm = OllamaLLM(model="llama3.1")
    db = SQLDatabase.from_uri(f"postgresql+psycopg2://{user}:{password}@{host}/{database}")

    presql_template = '''Question: {question}
    Answer below only from informations in the question. 
    Output (without explanation or additional note):
    Category (mandatory): (could be 'Bags, Wallets & Belts', 'Footwear', 'Toys', 'Clothing and Accessories')
    product name (mandatory): (main object without adjectives could be shoes, t-shirt, pants, e.t.c)
    Subject (if specified): (could be men, women, boy, girl, kid, unisex, e.t.c)
    Color (if specified): (could be red, green, blue, multicolor, e.t.c)
    Brand (if specified): (leave it blank if not mentioned)

    note: Ignore any additional information
    '''
    sql_template = '''Information: {input}
    Formulate sql query in {dialect} to retrieve relavant records based on information above. After that execute the query.
    Guidelines:
    1. informations and **columns** for WHERE clause filtering:
        - Category --> **category**: only consist of 'Bags, Wallets & Belts', 'Footwear', 'Toys', 'Clothing and Accessories'.
        - Product name --> **title**: Ex. WHERE title ILIKE '%t-shirts%'.
        - subject --> **product_details**: Ex. WHERE product_details::text ILIKE '%kid%'. (Must follow the structure from the example)
        - Color --> **product_details**: Ex. WHERE product_details::text ILIKE '%purple%'. (Must follow the structure from the example)
        - Brand --> **brand**: Ex. WHERE brand ILIKE '%brand%'.
    2. Columns to select: UUID, title, url
    3. Tables information:
    {table_info}
    sql example: 
    SELECT TOP {top_k} titles FROM products
    WHERE category = 'Footwear' 
        AND title ILIKE '%shoe%'
        AND product_details::text ILIKE '%Women%'
        AND product_details::text ILIKE '%purple%'

    Output: SQL query only without explantion or additional text
    '''

    custom_sqlprompt = PromptTemplate(
        input_variables=["input", "table_info", "top_k", "dialect"],
        template=sql_template
    )

    answer_prompt = PromptTemplate.from_template(
        """Given the following user question and the corresponding SQL result, identify up to 5 items from the result that are the most relevant to the question.
        
        The SQL result consists of a list of tuples where each tuple contains an item ID, a product name, and a URL.

        Question: {question}
        SQL Result: {result}
        
        Answer with a list of the most relevant items (maximum 5), formatted as:
        - Product name: [product name], URL: [product URL]
        
        Do not explain or describe the data, just provide the list of relevant items based on the question."""
    )

    presql_prompt_template = PromptTemplate(template=presql_template)

    write_query = create_sql_query_chain(llm, db, prompt=custom_sqlprompt)

    execute_query = QuerySQLDataBaseTool(db=db)

    chain = RunnablePassthrough.assign(information = presql_prompt_template | llm).assign(
            result={"question" : itemgetter("information")} 
            | write_query
            | RunnableLambda(lambda x: x.strip('```').strip('sql'))
            | execute_query
        ) | answer_prompt | llm

    response = chain.invoke({"question": question})
    return response

@app.route("/post_question", methods=["POST"])
def questionPost():
    json_content = request.json
    query = json_content.get("query")

    response_answer = recommendation(query)
    return response_answer


def start_app():
    app.run(host="0.0.0.0", port=8080, debug=True)


if __name__ == "__main__":
    start_app()