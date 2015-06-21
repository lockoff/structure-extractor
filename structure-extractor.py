import re
from datetime import datetime
from elasticsearch import Elasticsearch
from functools import partial
import sys
from tika import parser
import os

converters = {
    "property_type": str,
    "building_type": str,
    "title": str,
    "land_size": lambda s: float(s.split(" ")[0]),
    "built_in": lambda d: datetime.strptime(d, "%Y"),
    "parking_type": str
}

# 0. Create an elasticsearch index for the documents.
# 1. Get a directory containing the documents.
# 2. For every file in that directory:
#   2(a). Use tika to transform the file content to JSON with metadata and extracted text.
#   2(b). Send an index request to elasticsearch to index the document.
def etl(data_dir):
    _, _, file_names = os.walk(data_dir).next()
    files = [os.path.join(data_dir, filename) for filename in file_names]
    parsed_files = [parser.from_file(doc_file) for doc_file in files]
    index_docs = flatten_parsed_files(parsed_files)
    structured_data = [get_structured_data(index_doc["content"]) for index_doc in index_docs]
    for data in structured_data:
        print data
    # for index_doc in index_docs:
    #     print ""
    #     print json.dumps(index_doc, indent=4, sort_keys=True)

    # es = Elasticsearch()
    # for file_name, index_doc in zip(file_names, index_docs):
    #     doc_id = re.sub('\..*$', '', file_name)
    #     es.index(index="structure-extractor", doc_type="realty", id=doc_id, body=index_doc)

def get_structured_data(content):
    structured_data = {}
    lines = [line for line in content.split("\n") if line]
    for idx, line in enumerate(lines):
        for key in converters.keys():
            if line.lower().find(key.replace("_", " ")) > -1:
                structured_data[key] = converters[key](lines[idx+1])


def flatten_parsed_files(parsed_files):
    for parsed_file in parsed_files:
        index_doc = parsed_file["metadata"]
        for key, value in index_doc.iteritems():
            index_doc[key] = value[0]
        index_doc["content"] = parsed_file["content"]
        yield index_doc

if __name__ == '__main__':
    etl(sys.argv[1])

