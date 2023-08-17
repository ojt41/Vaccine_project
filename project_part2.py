import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import os

f = os.path.dirname(__file__) + "\\vaccine-distribution-data.xlsx"
data = pd.ExcelFile(f)
dfs = {sheet_name: data.parse(sheet_name) 
          for sheet_name in data.sheet_names}
#print(dfs)

try:
    
    database="grp18db_2023"    
    user="grp18_2023"    
    password="6BjZg4fv"   
    host="dbcourse.cs.aalto.fi"
    port = "5432"
    connection = psycopg2.connect(
                                        database=database,              
                                        user=user,       
                                        password=password,   
                                        host=host,
                                        port = port
                                    )
    connection.autocommit = True
    cursor = connection.cursor()

    DIALECT = 'postgresql+psycopg2://'
    db_uri = "%s:%s@%s/%s" % (user, password, host, database)

    engine = create_engine(DIALECT + db_uri)
    psql_conn  = engine.connect()
    if not psql_conn:
        print("DB connection is not OK!")
        exit()
    else:
        print("DB connection is OK.")

    keys = list(dfs.keys())

    dfs['Diagnosis']['date'] = dfs['Diagnosis']['date'].apply(str).apply(pd.to_datetime, errors = "coerce") # filtering the right date format 
    dfs['Diagnosis'] =dfs['Diagnosis'].dropna()                                                             # in Diagnosis table
    for key in keys:
        name = key.lower()
        dfs[key] = dfs[key].rename(str.lower, axis='columns') 
        dfs[key] = dfs[key].rename(str.strip, axis='columns') 
        dfs[key].to_sql(name = name, con=psql_conn, if_exists='append', index = False)
        
    
except Exception as e:
        print ("FAILED due to:" + str(e)) 




