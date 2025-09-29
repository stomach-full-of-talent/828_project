import io
import os
import re
from PyPDF2 import PdfReader,PdfWriter
import requests
import json
from dotenv import load_dotenv
import mimetypes
from concurrent.futures import ThreadPoolExecutor,as_completed
load_dotenv(verbose=True)
Dify_Dataset_Key = os.getenv('Dify_Dataset_Key',default=None)
Dify_Upload_App_Key = os.getenv('Dify_Upload_App_Key',default=None)
dataset_header = {'Authorization': f'Bearer {Dify_Dataset_Key}','Content-Type': 'application/json'}
app_header = {'Authorization': f'Bearer {Dify_Upload_App_Key}'}
base_url = 'http://aiserver.adw.com/v1'
#读取本地文件，用树的方式存储文件和路径
base_workspace = './公司数据'
def read_paths(dir):
    path_tree = os.walk(dir)
    for path, dir_lst, file_lst in path_tree:
        for f in file_lst:
            fullname = os.path.join(path, f)
            fullname = fullname.replace('\\','/')
            yield fullname

def find_dataset(path:str):
    dataset_id = None
    if re.search('君合境内',path):
        if re.search('01 基本情况文件',path):
            dataset_id = 'a6344db9-7364-476f-b55e-2924275043d4'
        elif re.search('02 业务资料',path):
            dataset_id = 'dedf88bd-7dc4-4e9b-81fb-8645de38b979'
        elif re.search('08 诉讼、仲裁及行政处罚',path):
            dataset_id = 'cb48709e-810e-4982-bd5a-99f3ca2b12de'
        elif re.search('12 尽职调查相关',path):
            dataset_id = 'f09c6132-6555-46d0-8d06-a8d7c0dbdc36'
    elif re.search('立信',path):
        dataset_id = '3648e9ae-e7f6-4b01-8b40-7da8ce73f14e'
    elif re.search('行业顾问',path):
        dataset_id = '74b5a3f4-692a-4f28-955f-5b7190f03d3e'
    elif re.search('招商国际',path):
        if re.search('1 基本情况文件',path) or re.search('3. 公司资产',path):
            dataset_id = 'a096265c-0f6f-41e9-b347-6d47b5779b24'
        elif re.search('4 公司债权债务及担保',path):
            dataset_id = '619fabdd-af22-4b44-a02c-d19c67a2edb3'
        elif re.search('5 财务及税务',path):
            dataset_id = '3220087f-3a81-45d5-9fcd-a586ef45cf1a'
        elif re.search('11 其他',path):
            dataset_id = '795ebd1c-4f0b-4903-b777-97ccd95bcbc5'
    elif re.search('招商证券',path):
        if re.search('01 子公司财务报表',path) or re.search('5 财务及税务',path):
            dataset_id = '8e08b5a0-9c82-4a45-9f56-d46fbb7d3c83'
        elif re.search('4 公司债权债务及担保',path):
            dataset_id = '9431acac-52a8-477a-a926-8b121ddedd01'
        elif re.search('大学合作项目',path):
            dataset_id = '7cde4227-a9e0-4b47-a691-52f919771d43'
    return dataset_id
paths = read_paths(base_workspace)
paths = list(paths)
if os.path.exists('./uploaded.txt'):
    with open('uploaded.txt','r',encoding='utf-8') as uploaded_file_list:
        uploaded =  uploaded_file_list.readlines()
        uploaded = [i.strip() for i in uploaded]
        paths = [p  for p in paths if p not in uploaded]
#由于含有大量文件需要上传，中途可能会发生中断，需要持久化存储已上传的文件记录，避免重复上传
def process_file(path):
    print(f'文件路径:{path}')
    try:
        with open(path, 'rb') as fp:
            file_name = os.path.basename(path)
            f_extension = re.match('^.(.*?).(\w+)$', file_name).groups()[1]
            # print(f_extension)
            if f_extension == 'zip':
                return
            if f_extension in ['jpg', 'jpeg', 'png', 'gif']:
                f_type = 'image'
            else:
                f_type = 'document'
            mime_type, _ = mimetypes.guess_type(path)

            if f_extension == 'pdf':
                doc_reader = PdfReader(fp)  # 打开文档
                page_number = len(doc_reader.pages)
                sub_file_list = []
                files = []
                f_id_list = []
                bio_list = []
                output_text = ''
                print(f'page_number:{page_number}')
                for i in range(page_number // 8 + 1):
                    start_page = i * 8
                    end_page = min((i + 1) * 8, page_number)
                    bio_list.append(io.BytesIO())
                    # 每8页分割一次PDF文件，避免单个文件过大超出LLM输入token限制
                    writer = PdfWriter()
                    writer.append(doc_reader, pages=(start_page, end_page))
                    # 通过BytesIO临时写入内存，然后直接上传，避免带来磁盘IO开销
                    writer.write(bio_list[i])
                    #writer.write(f'sub_{i + 1}_' + file_name)
                    writer.close()
                    bio_list[i].seek(0)
                    sub_file_list.append(bio_list[i])
                    files.append({'file': (f'sub_{i + 1}_' + file_name, sub_file_list[i], mime_type)})

                    response = requests.post(url=base_url + '/files/upload', headers=app_header, data={'user': 'admin'},
                                             files=files[i])
                    if response.status_code != 201:
                        raise ConnectionError(response.text)
                    response_text = json.loads(response.text)
                    f_id_list.append(response_text['id'])
                    input_files = [{
                        'type': f_type,
                        'transfer_method': 'local_file',
                        'upload_file_id': f_id_list[i]
                    }]
                    dataset_id = find_dataset(path)
                    inputs = {'input_files': input_files, 'path': path, 'dataset_id': dataset_id}
                    workflow_header = {'Authorization': f'Bearer {Dify_Upload_App_Key}',
                                       'Content-Type': 'application/json'}
                    body = {'user': 'admin', 'inputs': inputs, 'response_mode': 'blocking'}
                    workflow_response = requests.post(base_url + '/workflows/run', headers=workflow_header,
                                                      data=json.dumps(body))
                    if workflow_response.status_code != 200:
                        raise ConnectionError(workflow_response.text)
                    workflow_data = workflow_response.json()['data']
                    if workflow_data['status'] != 'succeeded':
                        raise ConnectionError(workflow_data['outputs']['error'])
                    output_text += workflow_data['outputs']['result']
                    print(workflow_data)

                for pdf_stream in bio_list:
                    pdf_stream.close()
                #print(f_id_list)
            else:
                files = {'file': (file_name, fp, mime_type)}
                response = requests.post(url=base_url + '/files/upload', headers=app_header, data={'user': 'admin'},
                                         files=files)
                if response.status_code != 201:
                    raise ConnectionError(response.text)
                response_text = json.loads(response.text)
                f_id = response_text['id']

                input_files = [{
                    'type': f_type,
                    'transfer_method': 'local_file',
                    'upload_file_id': f_id
                }]
                dataset_id = find_dataset(path)
                inputs = {'input_files': input_files, 'path': path, 'dataset_id': dataset_id}
                workflow_header = {'Authorization': f'Bearer {Dify_Upload_App_Key}',
                                   'Content-Type': 'application/json'}
                body = {'user': 'admin', 'inputs': inputs, 'response_mode': 'blocking'}
                workflow_response = requests.post(base_url + '/workflows/run', headers=workflow_header,
                                                  data=json.dumps(body))
                if workflow_response.status_code != 200:
                    raise ConnectionError(workflow_response.text)
                workflow_data = workflow_response.json()['data']
                print(workflow_data)
                output_text = workflow_data['outputs']['result']
                # print(workflow_data)
                if workflow_data['status'] != 'succeeded':
                    raise ConnectionError(output_text['error'])

            #将结果存储为本地文件
            json_output = {
                'output_text': output_text,
                'path': path,
                'dataset_id': dataset_id
            }
            clear_name, _ = os.path.splitext(file_name)
            dir = os.path.dirname(path).replace('./公司数据', '')
            if not os.path.exists('./processed_data' + dir):
                os.makedirs('./processed_data' + dir)
            with open(f'./processed_data{dir}/{clear_name}.json', 'w') as pf:
                json.dump(json_output, pf,indent=6)
            with open('uploaded.txt', 'a',encoding='utf-8') as uploaded_file_list:
                uploaded_file_list.writelines(path + '\n')
        return path
    except ConnectionError as e:
        print(e)
        with open('failed_files_log.txt','a') as log:
            log.writelines(path+'\n')
        raise e
if __name__ == '__main__':
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = [executor.submit(process_file,path) for path in paths]
        for future in as_completed(futures):
            try:
                print(f'file {future.result(timeout=1800)} finished')
            except TimeoutError:
                print('task can\'t finished in 30 minutes')
