import boto3

#executing the query
def queryExecution(athena,queryString,queryExeContex,resConfig):
    try:
        # passing the query string to athena.start_query_execution
        response = athena.start_query_execution(
        QueryString=queryString,
        QueryExecutionContext=queryExeContex,
        ResultConfiguration = resConfig
        
    )
        # storing query execution id
        query_execution_id = response['QueryExecutionId']
        query_status = 'QUEUED'
        response = None

        # Wait for the query to complete
        while query_status in ('QUEUED', 'RUNNING'):
            #geting qyery execution state
            response = athena.get_query_execution(QueryExecutionId=query_execution_id)
            query_status = response['QueryExecution']['Status']['State']
        # if succeed returning the query_execution_id for queryresults
        if query_status == 'SUCCEEDED':
            return query_execution_id
        
        return False
    except Exception as e:
        return False
# fetching the results of executed query
def QueryResults(athena,query_execution_id):
    if query_execution_id:
        response = athena.get_query_results(QueryExecutionId=query_execution_id)
        rows = response['ResultSet']['Rows']
        # Extract and print the data from the rows
        for row in rows:
            data = [field['VarCharValue'] for field in row['Data']]
            print(data)




# encapsulating all above function in one for passing to pythonoperator in airflow
def athena_fetching(access_key,secret_key,databaseconfig,s3config,Queries):
    
    access_key = access_key
    sceret_key = secret_key
    db = databaseconfig['databaseName']
    execution_context = {'Database': db}
    outputlocation = s3config['outputlocation']
    config = {'OutputLocation':outputlocation}
    # providing access key and secret key to creating boto3 session
    session = boto3.Session(aws_access_key_id=access_key,\
                            aws_secret_access_key=sceret_key)
    # creating athena client
    athena = session.client("athena")
    # AWS Glue Data Catalog is the default catalog
    response = athena.list_databases(CatalogName='AwsDataCatalog') 
    avilableDB = []
    # storing avilable in AWS Datacatalog
    for database in response['DatabaseList']:
        avilableDB.append(database['Name'])
        
    # checking if Db exist in AWS Data Catalog if not exist creating
    if db not in response['DatabaseList']:
        response = athena.start_query_execution(QueryString = f'create database {db}',
                                            ResultConfiguration = config)


    create_table_query = Queries['create_table_query']
    partitioning_query = Queries['partitioning_query']
    top_10_state_yearly_positive_query = Queries['top_10_state_yearly_positive_query']
    top_10_state_yearly_death_query = Queries['top_10_state_yearly_death_query']
    top_10_state_yearly_recovery_query = Queries['top_10_state_yearly_recovery_query']


    # Execute the query to create the table
    Queries = [top_10_state_yearly_positive_query,\
               top_10_state_yearly_death_query, \
               top_10_state_yearly_recovery_query]
    if queryExecution(athena,create_table_query,execution_context,config):
        if queryExecution(athena,partitioning_query,execution_context,config):
            for query in Queries:
                queryExecutionId = queryExecution(athena,query,execution_context,config)
                QueryResults(athena,queryExecutionId)





