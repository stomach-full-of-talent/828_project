import requests
from dotenv import load_dotenv
import os
import json
from files_upload2dify import read_paths
load_dotenv(verbose=True)
Dify_Dataset_Key = os.getenv('Dify_Dataset_Key',default=None)
def upload2dataset(json_path):
    with open(json_path,'r') as fp:
        file_dict = json.load(fp)
        full_text = file_dict['output_text']
        path = file_dict['path']
        dataset_id = file_dict['dataset_id']
        #print(full_text)
        dataset_header = {'Authorization': f'Bearer {Dify_Dataset_Key}',
                          'Content-Type': 'application/json'}
        base_url = 'http://aiserver.adw.com/v1'
        file_name = os.path.basename(path)
        input_data = {
            'name': file_name,
            'text': full_text,
            'indexing_technique': 'high_quality',
            'doc_form': 'hierarchical_model',
            'doc_language': '中文',
            'process_rule': {
                "mode": "hierarchical",
                "rules": {
                    'pre_processing_rules':[{
                        'id':'remove_extra_spaces',
                        'enabled':False
                    },{
                        'id':'remove_urls_emails',
                        'enabled':True
                    }],
                    "segmentation": {
                        'separator': '\n\n\n',
                        "max_tokens": 1024},
                    "parent_mode": "paragraph",
                    "subchunk_segmentation": {
                        'separator': '\n',
                        "max_tokens": 512,
                        "chunk_overlap": 128}
                }
            },
            "retrieval_model": {
                "search_method": "hybrid_search",
                "reranking_enable": True,
                "top_k": 10,
                "score_threshold_enabled": False
            }
        }
        try:
            response = requests.post(url=base_url + f'/datasets/{dataset_id}/document/create-by-text',
                                 headers=dataset_header, data=json.dumps(input_data))
            if response.status_code !=200:
                raise ConnectionError(response.text)
            document_id = json.loads(response.text)['document']['id']
            with open('in_dataset.txt', 'a', encoding='utf-8') as files_in_dataset:
                files_in_dataset.writelines(path + '\n')
            print(f'file {json_path} finished')
            print(response.text)
        except Exception as e:
            print(e)
            raise e

if __name__ == '__main__':
    upload_space = './processed_data'
    paths = read_paths(upload_space)
    if os.path.exists('./in_dataset.txt'):
        with open('in_dataset.txt', 'r', encoding='utf-8') as files_in_dataset:
            uploaded = files_in_dataset.readlines()
            uploaded = [i.strip() for i in uploaded]
            paths = [p for p in paths if p not in uploaded]
    for json_path in paths:
        upload2dataset(json_path)
