#!/usr/bin/python

from flask import Flask,render_template,request,redirect
import json
from flask import jsonify

app = Flask(__name__)


# Install the latest master of Haystack
#pip install grpcio-tools==1.34.1
#pip install git+https://github.com/deepset-ai/haystack.git

#pip install jsonlines

# Here are the imports we need

from haystack.file_converter.txt import TextConverter
from haystack.file_converter.pdf import PDFToTextConverter
from haystack.file_converter.docx import DocxToTextConverter

from haystack.preprocessor.preprocessor import PreProcessor
from haystack import Finder
from haystack.preprocessor.cleaning import clean_wiki_text
from haystack.preprocessor.utils import convert_files_to_dicts, fetch_archive_from_http
from haystack.reader.farm import FARMReader
from haystack.reader.transformers import TransformersReader
from haystack.utils import print_answers

from typing import List
import requests
import pandas as pd
from haystack import Document
from haystack.document_store.faiss import FAISSDocumentStore
from haystack.generator.transformers import RAGenerator
from haystack.retriever.dense import DensePassageRetriever
import jsonlines

from typing import Any, Dict, List, Optional

from haystack import Document
import random
import regex as re

import random
from flask import Flask,render_template,request,redirect
import json
from flask import jsonify

app = Flask(__name__)

# In Colab / No Docker environments: Start Elasticsearch from source


# Connect to Elasticsearch

from haystack.document_store.elasticsearch import ElasticsearchDocumentStore
document_store = ElasticsearchDocumentStore(host="10.0.0.36", port = "9200", username="", password="", index="document")






# Get the context for each question

def context_retriever(questions):

## Taking the input document.


    
    
    in_reader = []
#Read the question from txt

#     with open("Retriever/input_data/questions.txt") as file_in:
#         QUESTIONS = []
#         for line in file_in:
#             QUESTIONS.append(line)
            
# Fine the context for the question

    for question in questions:
      a = retriever.retrieve(query=question, top_k = 1)

      flat_docs_dict: Dict[str, Any] = {}
      for document in a:
          for k, v in document.__dict__.items():
            # print(v)
              if k not in flat_docs_dict:
                  flat_docs_dict[k] = []
              flat_docs_dict[k].append(v)
      print(flat_docs_dict)
      in_context = ' '.join(flat_docs_dict['text'])

      # Disct for Xlnet input
      x_in =  {'context': 'context input', 'qas': [{'question': "question", 'answers': [], 'qid': 'update', 'question_tokens': [], 'is_impossible': False, 'detected_answers': [{'text': '', 'token_spans': [], 'char_spans': []}]}]}
      #get the detials from the customer
      x_in['context'] = in_context
      for s in x_in['qas']:     
            s['qid'] = ''.join(random.choice('0123456789ABCDEF') for i in range(16))
            s['question'] = question
    
      in_reader.append(x_in)
    
    
    # Get the list of context 

    return in_reader


# if __name__ == "__main__":
#     questions1 = ['What is BERT', 'What is transformer?']
#     questions2 = ['How fast is bert', 'What is lstm?']
#     context_list1 = context_retriever(questions1)
#     context_list2 = context_retriever(questions2)
#     print(context_list1)
#     print("###########################################################################################")
#     print(context_list2)

    
    
    
    
    
    
    
    
    
    
    
    
# @app.route('/get_context', methods=['GET'])
# def get_context():
#     params = request.json
#     questions = ['What is BERT']
#     context_list = context_retriever(questions)
#     print(context_list)
#     return json.dumps(context_list)

# if __name__ == "__main__":   
#     app.run(host = "0.0.0.0", port=6786, debug = True)
#     questions = ['What is BERT']
#     context_list = context_retriever(questions)
#     print(context_list)SS    
    
    
    
    
    


@app.route('/get_context', methods=['GET'])
def get_context():
    params = request.json
    #print(params)
    questions = params["questions"]
    context_list = context_retriever(questions)
    return json.dumps(context_list)

if __name__ == "__main__":
    all_docs = convert_files_to_dicts(dir_path="/home/jovyan/MRC/Pipeline/Retreiver/input_data") # access the data
    preprocessor = PreProcessor(
        clean_empty_lines=True,
        clean_whitespace=True,
        clean_header_footer=False,
        split_by="word",
        split_length=150,
        split_respect_sentence_boundary=True
    )
    nested_docs = [preprocessor.process(d) for d in all_docs]
    docs = [d for x in nested_docs for d in x]

    #print(f"n_files_input: {len(all_docs)}\nn_docs_output: {len(docs)}")

    document_store.delete_all_documents() # delete the existing doc if any in document_store 
    document_store.write_documents(docs)

    # call retriever
    from haystack.retriever.sparse import ElasticsearchRetriever
    retriever = ElasticsearchRetriever(document_store=document_store)
    app.run(host = "0.0.0.0", port=9002)
