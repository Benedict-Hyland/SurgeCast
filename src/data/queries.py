import logging
import connectorx as cx
import pandas as pd
import credentials

base_dir, config_file = credentials.get_file()

logging.basicConfig(filename=f'{base_dir}/logs/get_data/queries.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info('Logger started')

def run_query(host, database, user, password, query=None):
    try:
        df = cx.read_sql(f"mysql://{user}:{password}@{host}/{database}", query)
        logging.info('Collecting data complete')
        return df
    except:
        logging.error(f'Error running query: {query}')
        
    
    

def get_counts_query(start_date, end_date):
    logging.debug(f'Getting counts query for start date: {start_date} and end date: {end_date}')
    query = f"""SELECT COM_DIST AS Postcode, FROM_UNIXTIME(
                        FLOOR(UNIX_TIMESTAMP(logonDate) / (3 * 60 * 60)) * (3 * 60 * 60)
                        ) AS DateTime
                FROM `claimsValidation` 
                WHERE isDuplicated = 'n' AND logonDate >= '{start_date}' AND logonDate <= '{end_date}' AND claimRef != 'demo' AND claimRef != 'test' 
    """
    logging.debug(f'Counts query: {query}')
    return query

def get_gfs_query(table_name):
    logging.debug(f'Getting GFS query for table: {table_name}')
    query = f"""SELECT 
    `Date` AS 'Date', 
    `Variable` AS 'Variable', 
    `Postcode` AS 'Postcode', 
    `Hour_0_3` AS '00', 
    `Hour_0_6` AS '03', 
    `Hour_0_9` AS '06', 
    `Hour_0_12` AS '09', 
    `Hour_0_15` AS '12', 
    `Hour_0_18` AS '15', 
    `Hour_0_21` AS '18', 
    `Hour_0_24` AS '21' 
    FROM `{table_name}`"""
    logging.debug(f'GFS Query: {query}')
    return query

