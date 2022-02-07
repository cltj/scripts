#!/usr/bin/env python3

##################################################################################################################
#| Henter data fra ms graph, legger på garanti informasjon og sender det videre til azure table storage. 		|#
#| DEVELOPED BY: cltj           						|#| Date:2022-02-06              						|#
##################################################################################################################
from azure.data.tables import TableClient, UpdateMode
from azure.core.exceptions import ResourceExistsError, HttpResponseError, ResourceNotFoundError
import requests
from datetime import datetime, date
import time
import pandas as pd
import json

"""
############## FOR PROD ################
import automationassets

CLIENT_ID = automationassets.get_automation_variable('CLIENT_ID')
TENANT_ID = automationassets.get_automation_variable('TENANT_ID')
CLIENT_SECRET = automationassets.get_automation_variable('CLIENT_SECRET')
CLIENT_SCOPE = automationassets.get_automation_variable('CLIENT_SCOPE')

access_key = automationassets.get_automation_variable('TABLES_PRIMARY_STORAGE_ACCOUNT_KEY')
connection_string = automationassets.get_automation_variable('AZURE_TABLES_CONNECTION_STRING')
account_name = automationassets.get_automation_variable('TABLES_STORAGE_ACCOUNT_NAME')
#########################################
"""
############# FOR DEV ##################
TENANT_ID= "6b3a1411-b00c-4757-a457-431727bbf88d"
CLIENT_ID="0b635cc5-d178-41aa-8623-c01ff18b722f"
CLIENT_SECRET="~5_7Q~pW2ZyeHEfD1TKDKQCNEjlCb01ZnByhY"
CLIENT_SCOPE="https%3A%2F%2Fgraph.microsoft.com%2F.default"
connection_string = "BlobEndpoint=https://lkintuneteststorage.blob.core.windows.net/;QueueEndpoint=https://lkintuneteststorage.queue.core.windows.net/;FileEndpoint=https://lkintuneteststorage.file.core.windows.net/;TableEndpoint=https://lkintuneteststorage.table.core.windows.net/;SharedAccessSignature=sv=2020-08-04&ss=t&srt=sco&sp=rwdlacu&se=2023-03-01T16:04:35Z&st=2022-02-05T08:04:35Z&spr=https&sig=IySKGg%2FV%2BjTk6RtX5tntazn%2BB1kdC2j88jJlOcBNSOs%3D"
#########################################


def get_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET, CLIENT_SCOPE):
    url = "https://login.microsoftonline.com/" + TENANT_ID + "/oauth2/v2.0/token"
    payload='grant_type=client_credentials&client_id=' + CLIENT_ID + \
            '&client_secret=' + CLIENT_SECRET + \
            '&scope=' + CLIENT_SCOPE
    headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cookie': 'fpc=AtM7Nn93KPlAjadNi4hsHg6zQUthAQAAAAXvj9kOAAAA; stsservicecookie=estsfd; x-ms-gateway-slice=estsfd'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    json_data = response.json()
    token = json_data['access_token']
    return token


def get_devices(url, headers, payload):
    lst = []
    response = requests.request("GET", url, headers=headers, data=payload)
    status_code = response.status_code
    if status_code == 200:
        json_data = response.json()
        if '@odata.nextLink' in json_data:
            odata_next_link = json_data['@odata.nextLink']
        odata_count = json_data['@odata.count']
        
        for device in json_data['value']:
            lst.append(device)
        if odata_count == 1000:
            get_devices(odata_next_link,headers,payload)
        else:
            sorted_results = sorted(lst, key=lambda d: d['enrolledDateTime'], reverse=False)
            return sorted_results
    else:
        res = "Error finding app assignments. Status code: " + str(status_code)
        return res


def do_call(TENANT_ID, CLIENT_ID, CLIENT_SECRET, CLIENT_SCOPE):
    token = get_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET, CLIENT_SCOPE)

    base_url = "https://graph.microsoft.com/v1.0/devicemanagement/managedDevices/"
    select_string = '$select=id, serialNumber, managedDeviceOwnerType, enrolledDateTime, model, manufacturer'
    filter_string = "$filter=operatingSystem eq 'Windows' AND managedDeviceOwnerType eq 'company' AND manufacturer eq 'LENOVO'"
    
    url = base_url + '?' + select_string + '&' + filter_string + '&$top=800'
    payload={}
    headers = {
        'Authorization': 'Bearer ' + token
        }
    return url, headers, payload


def entity_crud(table_name, operation, entity):
    with TableClient.from_connection_string(connection_string, table_name) as table_client:
        if operation == 'create':
            try:
                response = table_client.create_entity(entity=entity)
                return(response)
            except ResourceExistsError:
                print("Entity already exists")
        elif operation == 'query':
            try:
                queried_entity = table_client.get_entity(partition_key=entity['PartitionKey'],row_key=entity['RowKey'])
                return (queried_entity)
            except HttpResponseError as e:
                print(e.message)
        elif operation == 'update':
            try:
                response = table_client.update_entity(mode=UpdateMode.MERGE, entity=entity)
                return response
            except HttpResponseError as e:
                print(e.message)
        elif operation == 'delete':
            try:
                response = table_client.delete_entity(partition_key=entity['PartitionKey'],row_key=entity['RowKey'])
                return (response)
            except ResourceNotFoundError:
                print("Entity does not exists")


def get_warrenty_table():
    lst = []
    with TableClient.from_connection_string(connection_string, table_name='testTable') as table_client:
        try:
            entities = list(table_client.list_entities(select='id, deviceSerial, enrolledDateTime, updated'))
            return entities
        except HttpResponseError as e:
                print(e.message)
        

def drop_duplicates(sorted_results):
    df_all = pd.DataFrame(sorted_results)
    df_100 = df_all.head(5) ###########################################   TAKE 5 #############################################
    d = df_100.drop_duplicates(subset=['serialNumber'],keep='last')
    result = d.to_json(orient="records")
    parsed = json.loads(result)
    return parsed
         

def check_warrenty_info(device):
    # Check is warrenty is true
    device['PartitionKey'] = 'testTable'
    device['RowKey'] = device['id']
    has_warrenty_info = entity_crud(table_name='testTable', operation='query', entity=device)
    result = has_warrenty_info['updated']
    return result


def add_entity(device):
    new_device = {}
    new_device['PartitionKey'] = 'testTable'
    new_device['RowKey'] = device['id']
    new_device['deviceSerial'] = device['serialNumber']
    new_device['enrolledDateTime'] = device['enrolledDateTime']
    new_device['updated'] = False
    entity_crud(table_name='testTable', operation='create', entity=new_device)
    time.sleep(0.5)
    return new_device


def update_warranty_info(device):
    ################################################ The what and how of getting warrantyInfo ############################
    warantyStartTime = datetime(2020, 12, 20)
    warantyEndTime = datetime(2023, 12, 19)
    device['warantyStartTime'] = warantyStartTime
    device['warantyEndTime'] = warantyEndTime
    device['updated'] = True
    updated_device = entity_crud(table_name='testTable', operation='update', entity=device)
    return updated_device


def compare_add_update(devices, az_table_devices):
    lst = []
    for device in devices:
        if device in az_table_devices:
            # Check if warranty is set
            check = check_warrenty_info(device)
            if check == True:
                pass
            else:
                update_warranty_info(device)
                lst.append(device['serialNumber'])
        else:
            # Add device to az_table
            add_entity(device)
            check = check_warrenty_info(device)
            if check == True:
                pass
            else:
                update_warranty_info(device)
                lst.append(device['serialNumber'])
    return lst


if __name__ == '__main__':
    call = do_call(TENANT_ID, CLIENT_ID, CLIENT_SECRET, CLIENT_SCOPE)
    ms_graph_result = get_devices(call[0],call[1],call[2],)
    sorted_devices = drop_duplicates(ms_graph_result)
    az_table_result = get_warrenty_table()
    job_result = compare_add_update(sorted_devices, az_table_result)
    print(str(len(job_result)) + " entities was updated with warranty information")