import json
import re
from datetime import datetime
import string
from elasticsearch import Elasticsearch
from functools import partial
import sys
from tika import parser
import os

converters = {
    "property_type": str,
    "building_type": str,
    "title": str,
    # "land_size": lambda s: float(remove_punc(s.split(" ")[0])),
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
    files = [os.path.join(data_dir, filename) for filename in file_names if filename.endswith('.pdf')]
    parsed_files = [parser.from_file(doc_file) for doc_file in files]
    index_docs = list(flatten_parsed_files(parsed_files))
    # structured_data = get_structured_data(index_docs[0]["content"])
    # print file_names[0]
    # print structured_data
    # structured_data = [get_structured_data(index_doc["content"]) for index_doc in index_docs]
    # for data in structured_data:
    #     print data
    section_heads = {'description', 'details', 'building', 'walk score' + u"\u00ae"}
    sub_section_heads = {
        'details': {'features', 'parking type', 'structures', 'total parking spaces', 'view', 'waterfront', 'zoning id'},
        'building': {'bathrooms (total)', 'fireplace', 'floor space', 'style'}
    }
    for index_doc in index_docs:
        lines = [clean_header_line(line) for line in index_doc['content'].split("\n") if line]
        sections = get_sections(lines, section_heads)
        for section_name, section_content in sections.iteritems():
            if section_name in sub_section_heads.keys() and section_content:
                print get_sections(section_content, sub_section_heads[section_name])
        # for index_doc in index_docs:
        #     get_sections(index_doc['content'], sections)


        # for index_doc in index_docs:
        #     print ""
        #     print json.dumps(index_doc, indent=4, sort_keys=True)

        # es = Elasticsearch()
        # print index_docs
        # for file_name, index_doc in zip(file_names, index_docs):
        #     print 'File %s being indexed' % file_name
        #     doc_id = re.sub('\..*$', '', file_name)
        #     es.index(index="structure-extractor", doc_type="realty", id=doc_id, body=index_doc)


def get_structured_data(content):
    print content
    structured_data = {"content": content}
    lines = [line for line in content.split("\n") if line]
    (structured_data["description"], filtered_lines) = filter_description(lines)
    # for line in filtered_lines:
    #     print line
    for idx, line in enumerate(filtered_lines):
        for key in converters.keys():
            if line.lower().find(key.replace("_", " ")) > -1:
                structured_data[key] = converters[key](filtered_lines[idx + 1])
    return structured_data


def clean_header_line(line):
    inflection_point = find_first_case_inflection(line)
    possible_header = line[0:inflection_point]
    if inflection_point > 0 and line == possible_header * 4:
        return possible_header
    return line


def find_first_case_inflection(line):
    lower_set = set(string.ascii_lowercase)
    upper_set = set(string.ascii_uppercase)
    for i, ch in enumerate(line):
        if i != len(line) - 1 and ch in lower_set and line[i + 1] in upper_set:
            return i + 1
    return len(line)


def get_sections(lines, section_heads):
    section_heads.add('preamble')

    # print lines
    # print section_heads

    sections = dict.fromkeys(section_heads)
    section_head_lines = []
    for idx, line in enumerate(lines):
        if line.lower() in section_heads:
            section_head_lines.append(idx)

    if len(section_head_lines) == 0:
        return {}
    sections['preamble'] = lines[:section_head_lines[0]]
    for j, section_head_line in enumerate(section_head_lines):
        if j != len(section_head_lines) - 1:
            sections[lines[section_head_line].lower()] = lines[section_head_line:section_head_lines[j + 1]]
        else:
            sections[lines[section_head_line].lower()] = lines[section_head_line:]

    return sections


def filter_description(lines):
    desc_start = lines.index('Description')
    desc_end = lines.index('Details')
    description = ' '.join(lines[desc_start + 1:desc_end])
    content = lines[:desc_start] + lines[desc_end + 1:]
    return description, content


def flatten_parsed_files(parsed_files):
    for parsed_file in parsed_files:
        index_doc = parsed_file["metadata"]
        for key, value in index_doc.iteritems():
            index_doc[key] = value[0]
        index_doc["content"] = parsed_file["content"]
        yield index_doc


def remove_punc(s):
    exclude = set(',')
    return ''.join(ch for ch in s if ch not in exclude)


if __name__ == '__main__':
    etl(sys.argv[1])
