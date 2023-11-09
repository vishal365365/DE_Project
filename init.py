url = 'https://github.com/datameet/covid19/tree/master/downloads/mohfw-backup/data_json'

# database credential
# databaseconfig = {
#     'host' :'localhost',
#     'user' : 'root',
#     'password':'Vishal@365',
#     'databaseName' :"covid",
#     'table' :"covid_data"
# }
databaseconfig = {
    'host' :'vishal.c7uee4aii5ml.ap-south-1.rds.amazonaws.com',
    'user' : 'admin',
    'password':'vishal365',
    'databaseName' :"covid",
    'table' :"covid_data"
}
error_log_location ='/home/vishal/Documents/DE/error_log.txt'

# -----------RDS variables-------------
rds_queries = {
    'use_database':f"use {databaseconfig['databaseName']}",

    'create_database':f"create database {databaseconfig['databaseName']}",

    'create_table':f"""CREATE TABLE {databaseconfig['table']} (state_code INT,date DATE,state VARCHAR(255),
    PRIMARY KEY (state_code, date),cured INT,death INT,positive INT)""",

    'insert': f"""INSERT INTO {databaseconfig['table']} (state_code, date, state, cured, death, positive)
    VALUES (%s, %s, %s, %s, %s, %s)
    """,
    'drop_table' : f"drop table {databaseconfig['table']}",
    'table_name' : databaseconfig['table']
}
# -----------spark variables-------------
app_name = "DESpark"
jars = ['/home/vishal/Downloads/jars/aws-java-sdk-bundle-1.11.375.jar',
        '/home/vishal/Downloads/jars/hadoop-aws-3.3.1.jar',
        '/home/vishal/Downloads/jars/jets3t-0.9.4.jar',
        '/home/vishal/Downloads/jars/mysql-connector-java-8.0.13.jar']
access_key = "AKIA5B5JY32ZMP3FDPPT"
secret_key = "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"
spark_master = "spark://ttnpl-5860:7077"
spark_script= "/home/vishal/Documents/DE/spark.py"
s3sparklocation = "s3a://vishal-test-365lk/abc/"

#------------athena variables------------
s3config_athena = {
    's3Datalocaton':"s3://vishal-test-365lk/abc/",
    'outputlocation':"s3://athena4864/"
}

athena_queries = {
    'create_table_query':f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {databaseconfig['table']} (
          state_code INT,
          date DATE,
          state STRING,
          cured INT,
          death INT,
          positive INT,
          newly_active INT,
          newly_cured INT,
          newly_death INT
        )
        PARTITIONED BY (year STRING,month STRING)
        STORED AS PARQUET
        LOCATION 's3://vishal-test-365lk/abc/';
        """,

        'partitioning_query' : f'MSCK REPAIR TABLE {databaseconfig["table"]}',

        'top_10_state_yearly_positive_query' : f"""
        select * from (select *, dense_rank() over (partition by year order by total_positive desc) 
        as rank from (select year, state ,state_code, sum(newly_active) as total_positive from 
        {databaseconfig["table"]} group by state,state_code, year) c) cc where rank<11
        """,

        'top_10_state_yearly_death_query' : f"""
        select * from (select *, dense_rank() over (partition by year order by total_death desc) 
        as rank from (select year, state ,state_code, sum(newly_death) as total_death from 
        {databaseconfig['table']} group by state,state_code, year) c) cc where rank<11
        """,

        'top_10_state_yearly_recovery_query' : f"""
        select * from (select *, dense_rank() over (partition by year order by total_recoverd desc) 
        as rank from (select year, state ,state_code, sum(newly_cured) as total_recoverd from 
        {databaseconfig['table']} group by state,state_code, year) c) cc where rank<11
        """

}