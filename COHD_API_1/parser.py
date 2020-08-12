import pandas as pd
import json
import os

PAIRED_CONCEPT_FILE_NAME = 'pc_sorted_test.txt'
PAIRED_CONCEPT_COLUMN_NAMES = ["dataset_id","concept_id_1","concept_id_2","concept_count","concept_prevalence","chi_square_t","chi_square_p","expected_count","ln_ratio","rel_freq_1","rel_freq_2"]
CONCEPT_XREF_FILE_NAME = 'concept_xref.json'
CHUNK_SIZE = 1000000
MAX_COMBOS = 100

def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def generate_results(concept_dict, xref_data_dict):
    assoc_id = int(concept_dict["concept_id_2"])
    current_result = {
            "associated_concept_id": assoc_id,
            "associated_concept_name": xref_data_dict[str(assoc_id)]["concept_name"],
            "associated_domain_id": xref_data_dict[str(assoc_id)]['domain_id'],
            "concept_count": int(concept_dict["concept_count"]),
            "concept_prevalence": concept_dict["concept_prevalence"],
            "dataset_id": concept_dict["dataset_id"],
            "chi_square_t": concept_dict["chi_square_t"],
            "chi_square_p": concept_dict["chi_square_p"],
            "ln_ratio": concept_dict["ln_ratio"],
            "rel_freq_1": concept_dict["rel_freq_1"],
            "rel_freq_2":concept_dict["rel_freq_2"]
    }
    return current_result 

def load_annotations(data_folder):

    paired_concept_url = os.path.join(data_folder,PAIRED_CONCEPT_FILE_NAME)
    concept_xref_url = os.path.join(data_folder,CONCEPT_XREF_FILE_NAME)
    
    with open(concept_xref_url) as f:
        xref_data = json.load(f)
    
    xref_data_dict = {}

    for x in xref_data:
        xref_data_dict[x["_id"]] = x

    paired_concepts_table_total = pd.read_csv(paired_concept_url, sep='\t', header=None, names= PAIRED_CONCEPT_COLUMN_NAMES, chunksize=CHUNK_SIZE)  
    first_chunk = True
    row_counter = 0
    row_total = file_len(paired_concept_url)
    extra_entry = False
    for chunk in paired_concepts_table_total:
        paired_concepts_table = chunk
        for i,j in paired_concepts_table.iterrows():
            row_counter = row_counter + 1
            current_id = int(j["concept_id_1"])
            if(first_chunk):
                last_id = int(paired_concepts_table.iloc[0]["concept_id_1"])
                current_results = []
                current_results.append(generate_results(paired_concepts_table.iloc[0],xref_data_dict))
                current_count = 1
                first_chunk = False
            elif((current_count < MAX_COMBOS) & (last_id == current_id) & (row_counter != (row_total - 1))):
                current_results.append(generate_results(j,xref_data_dict))
                current_count = current_count + 1
            elif((last_id != current_id) | (i == (row_total - 1))):
                last_id_results = current_results
                if((current_count < MAX_COMBOS)&(last_id == current_id)&(i == (row_total - 1))):
                    last_id_results.append(generate_results(j,xref_data_dict))
                elif((last_id != current_id) & (i == (row_total - 1))):
                    extra_entry = True
                    extra_dict = {
                        "_id": str(current_id),
                        "concept_name": xref_data_dict[str(current_id)]["concept_name"],
                        "domain_id": xref_data_dict[str(current_id)]["domain_id"],
                        "xrefs": xref_data_dict[str(current_id)]["xrefs"],
                        "results": [generate_results(j,xref_data_dict)]
                    }
                else:
                    current_results = []
                    current_results.append(generate_results(j,xref_data_dict))
                    current_count = 1
                current_dict = {
                    "_id": str(last_id),
                    "concept_name": xref_data_dict[str(last_id)]["concept_name"],
                    "domain_id": xref_data_dict[str(last_id)]["domain_id"],
                    "xrefs": xref_data_dict[str(last_id)]["xrefs"],
                    "results": last_id_results
                }
#                 print(current_dict)
                yield(current_dict)
            last_id = current_id
        if(extra_entry):
#             print(extra_dict)
            yield(extra_dict)
