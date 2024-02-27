import requests
import yaml
import json
import os
import logging
import sys
import time
sys.path.append("/root/airflow/")
from urllib.parse import quote

domain = os.environ["domain"]
token = os.environ["token"]

# method to check file in the directory
def check_file(file_name,directory):

    # checking if file exist or not
    try:
        if not os.path.exists(directory):
            raise FileNotFoundError(f"Directory '{directory}' not found.")
        
        files = os.listdir(directory)

        # List all files in the directory
        files = os.listdir(directory)

        # Check if the file is in the list
        if file_name in files:
            print(f"File '{file_name}' found in directory '{directory}'.")
            return(directory + file_name)
        else:
            logging.error(f"File '{file_name}' not found in directory '{directory}'.")
            raise FileNotFoundError(f"File '{file_name}' not found in directory '{directory}'.")

    except FileNotFoundError as e:
        logging.error(f"Error: {e}")
        raise e

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise e


def get_request(url, headers=None):
    try:
        response = requests.get(url, headers=headers)
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            return response # Assuming the response is JSON
        else:
            print(f"Request failed with status code: {response.status_code}")
            return response
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred: {e}")
        raise e


def post_request(url, headers, body):
    try:
        response = requests.post(url, headers=headers, data=body)
        # Check if the response was successful (status code between 200 and 299)
        if response.status_code >= 200 and response.status_code < 300:
            return response  # Return the JSON response
        else:
            print(f"POST request failed with status code {response.status_code}: {response.text}")
            return response
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred during the POST request: {e}")
        raise e
    

def delete_request(url, headers=None):
    try:
        response = requests.delete(url, headers=headers)
        # Check if the request was successful (status code 200)
        if response.status_code == 200 or response.status_code == 204:
            return response  # Assuming the response is JSON
        else:
            raise Exception(f"failed to delete existing workflow")
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred: {e}")
        raise e


def load_yaml_to_dict(yaml_file):
    try:
        with open(yaml_file, 'r') as file:
            yaml_data = yaml.safe_load(file)
        return yaml_data
    except FileNotFoundError:
        logging.error("Error: File not found.")
        raise Exception
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML: {e}")
        raise Exception
    

def check_stack(stack):
    url = f"https://{domain}/poros/api/v1/resolve?stack={stack}"
    header={
    "Authorization" : f"Bearer {token}",
    "User-Agent": "dataos-ctl/2.18.5-dev"
    }
    response = get_request(url=url,headers=header)
    if response.status_code != 200:
        raise Exception(f"{stack} stack not supported")


def validate_apply(yaml_json,api=""):
    url = f"https://{domain}/poros/api/v1/workspaces/public/resources{api}"
    header={
    "Accept":"application/json",
    "Authorization" : f"Bearer {token}",
    "User-Agent": "dataos-ctl/2.18.5-dev",
    "Content-Type": "application/json"
    }
    response = post_request(url=url , headers=header , body=yaml_json)
    if not (response.status_code >= 200 and response.status_code < 300):
        raise Exception(f"validation failed")

def check_workflow(workflow):
    url = f"https://{domain}/poros/api/v1/workspaces/public/resources/workflow/{workflow}"
    header={
    "Authorization" : f"Bearer {token}",
    "User-Agent": "dataos-ctl/2.18.5-dev"
    }
    try:
        response = requests.get(url, headers=header)
        if response.status_code == 200:
            delete_request(url=url , headers=header)
        else:
            print(f"workflow does not exist")

    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred: {e}")
        raise e

# method to check the stus of workflow
def get_status_of_workflow(api_url,retry):
    try:
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

        # Make the GET request
        response = requests.get(api_url, headers=headers)

        # Check the response status code
        if response.status_code == 200:
            # Assuming the response is JSON, you can access the data using response.json()
            data = response.json()
            for i in range(0 , len(data["nodeDetails"])):
                print(f"Name : {data['nodeDetails'][i]['name']}  ,  AppSpecName : {data['nodeDetails'][i]['appSpecName']}  ,  PodName : {data['nodeDetails'][i]['podName']}  ,  Type : {data['nodeDetails'][i]['type']}  ,  Phase : {data['nodeDetails'][i]['phase']}\n\n")
            return data["runtimeStatus"]
        
        elif retry > 0:
            retry -= 1
            return retry
        
        elif response.status_code == 404:
            print("Error applying workflow...")
            raise Exception("Error applying workflow")

        else:
            print(f"GET request failed with status code: {response.status_code}")
            print(f"Response content: {response.text}")
            raise Exception(f"Response content: {response.text}")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise e


# ========================================================================================
# ========================================================================================
def submit_job(file_name):
    # checking file...
    directory = "/root/airflow/workflow/"
    yaml_file = check_file(file_name,directory)

    # loading workflow_yaml
    yaml_dict = load_yaml_to_dict(yaml_file)

# ====================================================
    # modifying yaml_dict
    yaml_dict["owner"] = "adityaudasi"
    yaml_dict["workspace"] = "public"
    yaml_dict["status"] = {}

# ==================================================== 
    # check stack and adding user
    # print("checking stack")
    for i in range(0,len(yaml_dict['workflow']['dag'])):
        flare_stack = quote(yaml_dict['workflow']['dag'][i]['spec']['stack'])
        yaml_dict['workflow']['dag'][i]['spec']['runAsUser'] = "adityaudasi"
        check_stack(flare_stack)
    
# =====================================================
    # validate
    # print("validating workflow")
    yaml_json = json.dumps(yaml_dict)
    validate_apply(yaml_json,"/validate")

# =====================================================
    # check if workflow already existed
    print("deleting existing workflow")
    workflow_name = yaml_dict['name']
    check_workflow(workflow_name)

# =====================================================
    # apply workflow
    print("deploying workflow")
    validate_apply(yaml_json)

    retry = 5
    while True:
        time.sleep(5)
        print(f"Name : {workflow_name}  ,  Type : Workflow  ,  Workspace : Public  ,  Owner :  adityaudasi\n")
        url = f"https://{domain}/poros/api/v1/workspaces/public/resources/workflow/{workflow_name}/runtime"
        response = get_status_of_workflow(url,retry)
        print(f"https://{domain}/operations/user-space/resources/resource-details?name={workflow_name}&type=workflow&workspace=public\n")

        if response == "succeeded":
            print("workflow Executed successfully")
            break
        elif response == "failed":
            raise Exception("Workflow failed unexpectedly")
        else: 
            retry = response




# apply_worflow()

    
