# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
meant to extract data from pdf into memory
"""

from pathlib import Path
from tqdm import tqdm
import pandas as pd
import PyPDF2
import os
import re
import json

sections = {
    "designed_for_table": "טבלת ייעודים",
}

data_type = {
    "client_name": {"query": ":לקוח שם", "type": "text"},
    "identification": {"query": ":ת.זהות מספר", "type": "text"},
    "account_number": {"query": "חשבון מספר", "type": "text"},
    "no_permission": {"query": "הרשא אין ", "type": "text"},
    "digits": {"query": "\\b[0-9]{20}\\b", "type": "regex"},
}


def hebrew_text_is_contained(s1, s2):
    ss1 = set([ord(x) for x in s1])
    ss2 = set([ord(x) for x in s2])
    return len(ss1.intersection(ss2)) / min(len(ss1), len(ss2)) >= 1


def extract_text(filepath):
    reader = PyPDF2.PdfFileReader(str(filepath))
    text = []
    for i in range(reader.numPages):
        t = reader.getPage(i).extractText().replace('\xa0', ' ').replace('\u200b', '')
        text.extend([(x, i) for x in str(t).split("\n")])

    non_empty_lines = [l for l in text if len(l[0]) > 0]
    return non_empty_lines


def is_hebrew(c):
    return ("\u0590" <= c <= "\u05EA")


def hebrew_strip(text):
    return "".join([c for c in text if is_hebrew(c) == True])


def compare_hebrew_strings(query, text):
    s1 = hebrew_strip(text)
    s2 = hebrew_strip(query)
    return s1.find(s2) >= 0 and len(s1) > 0 and len(s2) > 0


def obtain_sections(lines, sections=sections):
    s_lines = pd.DataFrame(lines, columns=["text", "page"])
    for s in sections:
        query = sections[s]
        s_lines[s] = s_lines.text.str.contains(query).astype(int).cumsum()

    for v in data_type:
        query = data_type[v]
        if query["type"] == "text":
            s_lines[v] = s_lines.text.str.contains(query["query"]).astype(int)
        elif query["type"] == "regex":
            s_lines[v] = s_lines.text.str.contains(query["query"]).astype(int)
    return s_lines


def is_numeric(text: str):
    return len(re.findall("[0-9]", text)) >= len(text) - 1


def obtain_returned_transactions(sections_dataframe: pd.DataFrame):
    data = sections_dataframe
    returned_transactions_df = data[(data.returned_transactions_list >= 1) &
                                    (data.digits == 1)].copy()
    returned_transactions = []
    for row_text in returned_transactions_df.text:
        row_data = [x for x in row_text.split() if is_numeric(x)]
        value = ([x for x in row_data if re.match("[0-9]{1,10}\.[0-9]{2}", x)] + [None])[0]
        if float(value) > 0:
            bank_n = row_data[1]
            branch_code = row_data[2]
            account_n = row_data[3]
            serial_n = row_data[4]
            returned_transactions.append({
                "value": remove_string_extra_spaces(value),
                "bank_number": remove_string_extra_spaces(bank_n),
                "branch_code": remove_string_extra_spaces(branch_code),
                "account_number": remove_string_extra_spaces(account_n),
                "serial_number": remove_string_extra_spaces(serial_n),
                "type": "returned_transaction"
            })
    return returned_transactions


# Remove all extra spaces
def remove_string_extra_spaces(string):
    return " ".join(str(string).split())


def obtain_confirmed_transactions(sections_dataframe: pd.DataFrame):
    data = sections_dataframe
    confirmed = data[(data.debit_confirmation_notice >= 1) &
                     data.client_name >= 1].copy()
    client_names = [x.split(data_type["client_name"]["query"])[0] for x in confirmed.text]
    client_names = [re.sub("[\s]{1,100}", " ", x) for x in client_names]
    client_ids = [x.split(data_type["client_name"]["query"]
                          )[1].split(data_type["identification"]["query"])[0]
                  for x in confirmed.text]
    client_ids = [re.sub("[\s]{1,100}", "", x) for x in client_ids]
    retrieved = [{"client_name": remove_string_extra_spaces(cl_name),
                  "client_id": remove_string_extra_spaces(cl_id)} for
                 cl_name, cl_id in zip(client_names, client_ids)]
    return retrieved


def allowed_file(filename, ALLOWED_EXTENSIONS):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def extract_data(filepath: str):
    lines = extract_text(filepath)
    sections_dataframe = obtain_sections(lines)
    returned_transactions = obtain_returned_transactions(sections_dataframe)
    debit_confirmations = obtain_confirmed_transactions(sections_dataframe)

    extracted_data = {"returned_transactions": returned_transactions,
                      "debit_confirmations": debit_confirmations}
    return extracted_data
