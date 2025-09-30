import os
import requests
import json
import time
import mimetypes
from dotenv import load_dotenv
load_dotenv(verbose=True)
Dify_Dataset_Key = os.getenv('Dify_Dataset_Key',default=None)
base_url = 'http://aiserver.adw.com/v1/'
dataset_header = {'Authorization': f'Bearer {Dify_Dataset_Key}','Content-Type': 'application/json'}
dataset_dict = {
    '君合境内_基本情况':'a6344db9-7364-476f-b55e-2924275043d4',
    '君合境内_业务资料':'dedf88bd-7dc4-4e9b-81fb-8645de38b979',
    '君合境内_诉讼仲裁':'cb48709e-810e-4982-bd5a-99f3ca2b12de',
    '君合境内_保密与竞业条例':'f09c6132-6555-46d0-8d06-a8d7c0dbdc36',
    '招商国际_资产和验资报告':'a096265c-0f6f-41e9-b347-6d47b5779b24',
    '招商国际_债务债权担保':'619fabdd-af22-4b44-a02c-d19c67a2edb3',
    '招商国际_财务税务':'3220087f-3a81-45d5-9fcd-a586ef45cf1a',
    '招商国际_员工相关':'795ebd1c-4f0b-4903-b777-97ccd95bcbc5',
    '招商证券_财务':'8e08b5a0-9c82-4a45-9f56-d46fbb7d3c83',
    '招商证券_债权债务担保':'9431acac-52a8-477a-a926-8b121ddedd01',
    '招商证券_大学合作项目':'7cde4227-a9e0-4b47-a691-52f919771d43'
}
def find_files_id(dataset_id):
    try:
        response = requests.request('GET', f'http://aiserver.adw.com/v1/datasets/{dataset_id}/documents',
                                    headers=dataset_header)
        if response.status_code != 200:
            raise ConnectionError(response)
        file_list = json.loads(response.text)['data']
        file_ids = []
        for i in range(len(file_list)):
            file_ids.append(file_list[i]['id'])
        return file_ids
    except ConnectionError as e:
        print(e)
        raise e

def change_rules(file_id,dataset_id):
    body = {
        'process_rule': {
                "mode": "custom",
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
                }
        }
    }
    try:
        response = requests.request('POST',url=base_url+f'datasets/{dataset_id}/documents/{file_id}/update-by-text',headers=dataset_header,json=body)
        if response.status_code !=200:
            raise ConnectionError(response.text)
        print(f'change success on file {file_id}')
    except ConnectionError as e:
        print(e)
        raise e
def main():
    for dataset_id in dataset_dict.values():
        files_id = find_files_id(dataset_id)
        for f_id in files_id:
            change_rules(f_id,dataset_id)
    time.sleep(1)
if __name__ == "__main__":
    main()
