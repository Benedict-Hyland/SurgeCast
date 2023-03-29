import configparser
import os
import pymysql
import pymysql.cursors
import pandas as pd

def get_file():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(os.getcwd()), '../'))
    config_file = os.path.join(base_dir, 'config' ,'database_credentials.ini')
    return base_dir, config_file

def get_configs(file, section='test'):

    config = configparser.ConfigParser()
    config.read(file)

    host = config.get(section, 'host')
    database = config.get(section, 'database')
    user = config.get(section, 'user')
    password = config.get(section, 'password')

    return host, database, user, password

def run_query(host, database, user, password, query):
    # Connect to the database
    connection = pymysql.connect(host=host,
                                 user=user,
                                 password=password,
                                 database=database,
                                 cursorclass=pymysql.cursors.DictCursor)
    
    try:
        df=pd.DataFrame(columns=['DateTime','Postcode','CC','CP','GST','HiT','HL','LoT','MWD','MWS','NCP','SD','SD_P','SwD','SwF','T','TP','Vs'])
        with connection.cursor() as cursor:
            cursor.execute(query)
            
            # Fetch and store the results in pandas dataframe in chunks
            while True:
                chunk = cursor.fetchmany(size=1000)
                if not chunk:
                    break
                df = df.append(chunk)
    finally:
        connection.close()
    return df

if __name__ == '__main__':
    base_dir, config_file = get_file()
    host, database, user, password = get_configs(file=config_file, section='test')
