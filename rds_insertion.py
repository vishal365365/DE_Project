import requests
from sql import databaseConnection,createSchema,inserting
from datacleanup import jsonError
import json

#fetching the file urls
def fetching_file_urls(url):
    files = {}
    response = requests.get(url)
    if response.status_code == 200:
        content = response.json()
        data = content['payload']['tree']['items']
        url_modified = url[:48]
        for file in data:
            date = file['name'][:10]
            files[date] = url_modified+file['path']
        files['2020-07-28'] = files['28-07-2020']
    return files


# fetching the json data from files and inserting it into rds
def inserting_data_todb(files,databaseObj,queries,error_log):
     # creating schema in DB
    createSchema(databaseObj,queries)
    #loading data one by one from url in files 
    for date in files:
        url = files[date]
        response = requests.get(url)
        data = response.json()['payload']['blob']['rawLines']
        cleaned_data = ''.join(data)
        try:
            data_dict = json.loads(cleaned_data)
        except json.decoder.JSONDecodeError as E:
            data_dict = jsonError(cleaned_data)

        # inserting this data to db
        inserting(databaseObj,data_dict,date,url,queries,error_log)

# encapsulating all above function in one for passing to pythonoperator in airflow
def data_fetching_and_inserting(url,databaseconfig,queries,error_log):
    files = fetching_file_urls(url)
    dbObj = databaseConnection(databaseconfig['host'],databaseconfig['user'],\
                               databaseconfig['password'])
    inserting_data_todb(files,dbObj,queries,error_log)
    
# -------------------------------------------------------------------------------------------



    