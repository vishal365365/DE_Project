from datacleanup import fix_state_code,fix_state_name,validate_data_type
import pymysql


#creating database connection 
def databaseConnection(host, user, password):
    db = pymysql.connect(host=host, user = user, password=password)
    return db

#creating database and table schema
def createSchema(dbObj,queries):
    cursor = dbObj.cursor()
    table = queries['table_name']
    #using database if exist otherwise creating
    try:
        cursor.execute(queries['use_database'])
    except pymysql.err.OperationalError as e:
        cursor.execute(queries['create_database'])
        cursor.execute(queries['use_database'])
    #getting list of tables and checking if table is present if present droping the table
    # and creating new one
    cursor.execute("show tables")
    tabel_list = cursor.fetchone()
    if tabel_list != None and table in tabel_list:
        cursor.execute(queries['drop_table'])
    cursor.execute(queries['create_table'])

#inserting data into Db date wise
def inserting(dbObj,data,date,filelocation,queries,error_log):
    cursor = dbObj.cursor()
    cursor.execute(queries['use_database'])
#    looping through the data and inserting one by one
    for row in data:
        print(row)
        try:
            #validating the data type according to column type
            validate_data_type(row,date)
            #fixing the ambiguous data of type state code if exist
            fix_state_code(row,date) 
            #fixing the ambiguous data of type state name if exist
            fix_state_name(row)
            #executing the query
            cursor.execute(queries['insert'], (row['state_code'], date,row['state_name'],\
                          row['cured'], row['death'], row['positive']))
            dbObj.commit()
        # if any value error occured catching the error
        except ValueError as e:
            dbObj.rollback()
            # opening the file in append mode and inserting the error log
            with open(error_log, 'a') as log_file:
                log_file.write(f"Record: {row}\n")
                log_file.write(f"File Location: {filelocation}\n\n")
        # if any primary key occured error logging it into log file
        except pymysql.err.IntegrityError as e:
            with open(error_log, 'a') as log_file:
                log_file.write(f"Record: {row}\n")
                log_file.write(f"File Location: {filelocation}\n\n")


        


