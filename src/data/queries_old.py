import logging
import pymysql
import pymysql.cursors
import pandas as pd
import credentials

base_dir, config_file = credentials.get_file()

logging.basicConfig(filename=f'{base_dir}/logs/get_data/queries.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info('Logger started')

def run_query(host, database, user, password, query):
    # Connect to the database using connectorx
    
    logging.info('Connected to database')
    try:
        chunks = []
        with connection.cursor() as cursor:
            cursor.execute(query)
            logging.debug('Executed query')
            
            # Fetch and store the results in pandas dataframe in chunks
            while True:
                chunk = cursor.fetchmany(size=100000)
                logging.debug('Fetched chunk')
                if not chunk:
                    break
                chunks.append(pd.DataFrame(chunk))
                logging.debug('Appended chunk to list')
        df = pd.concat(chunks, ignore_index=True)
        logging.debug('Concatenated chunks to dataframe')
        del chunks  # Del deletes the variable from memory to free up space as it gets quite large
        logging.debug('Deleted chunks from memory')
    except:
        logging.error(f'Error running query: {query}')
    finally:
        connection.close()
        logging.debug('Closed connection to database')
    logging.info('Collecting data complete')
    return df

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
    query = f"""
        SELECT STR_TO_DATE(CONCAT(Date, ' ', '00'), '%Y-%m-%d %H') AS DateTime, Postcode,
            MAX(CASE WHEN Variable = 'CC' THEN `Hour_0_3` END) AS CC,
            MAX(CASE WHEN Variable = 'CP' THEN `Hour_0_3` END) AS CP,
            MAX(CASE WHEN Variable = 'GST' THEN `Hour_0_3` END) AS GST,
            MAX(CASE WHEN Variable = 'HiT' THEN `Hour_0_3` END) AS HiT,
            MAX(CASE WHEN Variable = 'HL' THEN `Hour_0_3` END) AS HL,
            MAX(CASE WHEN Variable = 'LoT' THEN `Hour_0_3` END) AS LoT,
            MAX(CASE WHEN Variable = 'MWD' THEN `Hour_0_3` END) AS MWD,
            MAX(CASE WHEN Variable = 'MWS' THEN `Hour_0_3` END) AS MWS,
            MAX(CASE WHEN Variable = 'NCP' THEN `Hour_0_3` END) AS NCP,
            MAX(CASE WHEN Variable = 'SD' THEN `Hour_0_3` END) AS SD,
            MAX(CASE WHEN Variable = 'SD_P' THEN `Hour_0_3` END) AS SD_P,
            MAX(CASE WHEN Variable = 'SwD' THEN `Hour_0_3` END) AS SwD,
            MAX(CASE WHEN Variable = 'SwF' THEN `Hour_0_3` END) AS SwF,
            MAX(CASE WHEN Variable = 'T' THEN `Hour_0_3` END) AS T,
            MAX(CASE WHEN Variable = 'TP' THEN `Hour_0_3` END) AS TP,
            MAX(CASE WHEN Variable = 'Vs' THEN `Hour_0_3` END) AS Vs
        FROM `{table_name}`
        GROUP BY Date, Postcode

        UNION ALL

        SELECT STR_TO_DATE(CONCAT(Date, ' ', '03'), '%Y-%m-%d %H') AS DateTime, Postcode,
            MAX(CASE WHEN Variable = 'CC' THEN `Hour_0_6` END) AS CC,
            MAX(CASE WHEN Variable = 'CP' THEN `Hour_0_6` END) AS CP,
            MAX(CASE WHEN Variable = 'GST' THEN `Hour_0_6` END) AS GST,
            MAX(CASE WHEN Variable = 'HiT' THEN `Hour_0_6` END) AS HiT,
            MAX(CASE WHEN Variable = 'HL' THEN `Hour_0_6` END) AS HL,
            MAX(CASE WHEN Variable = 'LoT' THEN `Hour_0_6` END) AS LoT,
            MAX(CASE WHEN Variable = 'MWD' THEN `Hour_0_6` END) AS MWD,
            MAX(CASE WHEN Variable = 'MWS' THEN `Hour_0_6` END) AS MWS,
            MAX(CASE WHEN Variable = 'NCP' THEN `Hour_0_6` END) AS NCP,
            MAX(CASE WHEN Variable = 'SD' THEN `Hour_0_6` END) AS SD,
            MAX(CASE WHEN Variable = 'SD_P' THEN `Hour_0_6` END) AS SD_P,
            MAX(CASE WHEN Variable = 'SwD' THEN `Hour_0_6` END) AS SwD,
            MAX(CASE WHEN Variable = 'SwF' THEN `Hour_0_6` END) AS SwF,
            MAX(CASE WHEN Variable = 'T' THEN `Hour_0_6` END) AS T,
            MAX(CASE WHEN Variable = 'TP' THEN `Hour_0_6` END) AS TP,
            MAX(CASE WHEN Variable = 'Vs' THEN `Hour_0_6` END) AS Vs
        FROM `{table_name}`
        GROUP BY Date, Postcode

        UNION ALL

        SELECT STR_TO_DATE(CONCAT(Date, ' ', '06'), '%Y-%m-%d %H') AS DateTime, Postcode,
            MAX(CASE WHEN Variable = 'CC' THEN `Hour_0_9` END) AS CC,
            MAX(CASE WHEN Variable = 'CP' THEN `Hour_0_9` END) AS CP,
            MAX(CASE WHEN Variable = 'GST' THEN `Hour_0_9` END) AS GST,
            MAX(CASE WHEN Variable = 'HiT' THEN `Hour_0_9` END) AS HiT,
            MAX(CASE WHEN Variable = 'HL' THEN `Hour_0_9` END) AS HL,
            MAX(CASE WHEN Variable = 'LoT' THEN `Hour_0_9` END) AS LoT,
            MAX(CASE WHEN Variable = 'MWD' THEN `Hour_0_9` END) AS MWD,
            MAX(CASE WHEN Variable = 'MWS' THEN `Hour_0_9` END) AS MWS,
            MAX(CASE WHEN Variable = 'NCP' THEN `Hour_0_9` END) AS NCP,
            MAX(CASE WHEN Variable = 'SD' THEN `Hour_0_9` END) AS SD,
            MAX(CASE WHEN Variable = 'SD_P' THEN `Hour_0_9` END) AS SD_P,
            MAX(CASE WHEN Variable = 'SwD' THEN `Hour_0_9` END) AS SwD,
            MAX(CASE WHEN Variable = 'SwF' THEN `Hour_0_9` END) AS SwF,
            MAX(CASE WHEN Variable = 'T' THEN `Hour_0_9` END) AS T,
            MAX(CASE WHEN Variable = 'TP' THEN `Hour_0_9` END) AS TP,
            MAX(CASE WHEN Variable = 'Vs' THEN `Hour_0_9` END) AS Vs
        FROM `{table_name}`
        GROUP BY Date, Postcode

        UNION ALL

        SELECT STR_TO_DATE(CONCAT(Date, ' ', '09'), '%Y-%m-%d %H') AS DateTime, Postcode,
            MAX(CASE WHEN Variable = 'CC' THEN `Hour_0_12` END) AS CC,
            MAX(CASE WHEN Variable = 'CP' THEN `Hour_0_12` END) AS CP,
            MAX(CASE WHEN Variable = 'GST' THEN `Hour_0_12` END) AS GST,
            MAX(CASE WHEN Variable = 'HiT' THEN `Hour_0_12` END) AS HiT,
            MAX(CASE WHEN Variable = 'HL' THEN `Hour_0_12` END) AS HL,
            MAX(CASE WHEN Variable = 'LoT' THEN `Hour_0_12` END) AS LoT,
            MAX(CASE WHEN Variable = 'MWD' THEN `Hour_0_12` END) AS MWD,
            MAX(CASE WHEN Variable = 'MWS' THEN `Hour_0_12` END) AS MWS,
            MAX(CASE WHEN Variable = 'NCP' THEN `Hour_0_12` END) AS NCP,
            MAX(CASE WHEN Variable = 'SD' THEN `Hour_0_12` END) AS SD,
            MAX(CASE WHEN Variable = 'SD_P' THEN `Hour_0_12` END) AS SD_P,
            MAX(CASE WHEN Variable = 'SwD' THEN `Hour_0_12` END) AS SwD,
            MAX(CASE WHEN Variable = 'SwF' THEN `Hour_0_12` END) AS SwF,
            MAX(CASE WHEN Variable = 'T' THEN `Hour_0_12` END) AS T,
            MAX(CASE WHEN Variable = 'TP' THEN `Hour_0_12` END) AS TP,
            MAX(CASE WHEN Variable = 'Vs' THEN `Hour_0_12` END) AS Vs
        FROM `{table_name}`
        GROUP BY Date, Postcode

        UNION ALL

        SELECT STR_TO_DATE(CONCAT(Date, ' ', '12'), '%Y-%m-%d %H') AS DateTime, Postcode,
            MAX(CASE WHEN Variable = 'CC' THEN `Hour_0_15` END) AS CC,
            MAX(CASE WHEN Variable = 'CP' THEN `Hour_0_15` END) AS CP,
            MAX(CASE WHEN Variable = 'GST' THEN `Hour_0_15` END) AS GST,
            MAX(CASE WHEN Variable = 'HiT' THEN `Hour_0_15` END) AS HiT,
            MAX(CASE WHEN Variable = 'HL' THEN `Hour_0_15` END) AS HL,
            MAX(CASE WHEN Variable = 'LoT' THEN `Hour_0_15` END) AS LoT,
            MAX(CASE WHEN Variable = 'MWD' THEN `Hour_0_15` END) AS MWD,
            MAX(CASE WHEN Variable = 'MWS' THEN `Hour_0_15` END) AS MWS,
            MAX(CASE WHEN Variable = 'NCP' THEN `Hour_0_15` END) AS NCP,
            MAX(CASE WHEN Variable = 'SD' THEN `Hour_0_15` END) AS SD,
            MAX(CASE WHEN Variable = 'SD_P' THEN `Hour_0_15` END) AS SD_P,
            MAX(CASE WHEN Variable = 'SwD' THEN `Hour_0_15` END) AS SwD,
            MAX(CASE WHEN Variable = 'SwF' THEN `Hour_0_15` END) AS SwF,
            MAX(CASE WHEN Variable = 'T' THEN `Hour_0_15` END) AS T,
            MAX(CASE WHEN Variable = 'TP' THEN `Hour_0_15` END) AS TP,
            MAX(CASE WHEN Variable = 'Vs' THEN `Hour_0_15` END) AS Vs
        FROM `{table_name}`
        GROUP BY Date, Postcode

        UNION ALL

        SELECT STR_TO_DATE(CONCAT(Date, ' ', '15'), '%Y-%m-%d %H') AS DateTime, Postcode,
            MAX(CASE WHEN Variable = 'CC' THEN `Hour_0_18` END) AS CC,
            MAX(CASE WHEN Variable = 'CP' THEN `Hour_0_18` END) AS CP,
            MAX(CASE WHEN Variable = 'GST' THEN `Hour_0_18` END) AS GST,
            MAX(CASE WHEN Variable = 'HiT' THEN `Hour_0_18` END) AS HiT,
            MAX(CASE WHEN Variable = 'HL' THEN `Hour_0_18` END) AS HL,
            MAX(CASE WHEN Variable = 'LoT' THEN `Hour_0_18` END) AS LoT,
            MAX(CASE WHEN Variable = 'MWD' THEN `Hour_0_18` END) AS MWD,
            MAX(CASE WHEN Variable = 'MWS' THEN `Hour_0_18` END) AS MWS,
            MAX(CASE WHEN Variable = 'NCP' THEN `Hour_0_18` END) AS NCP,
            MAX(CASE WHEN Variable = 'SD' THEN `Hour_0_18` END) AS SD,
            MAX(CASE WHEN Variable = 'SD_P' THEN `Hour_0_18` END) AS SD_P,
            MAX(CASE WHEN Variable = 'SwD' THEN `Hour_0_18` END) AS SwD,
            MAX(CASE WHEN Variable = 'SwF' THEN `Hour_0_18` END) AS SwF,
            MAX(CASE WHEN Variable = 'T' THEN `Hour_0_18` END) AS T,
            MAX(CASE WHEN Variable = 'TP' THEN `Hour_0_18` END) AS TP,
            MAX(CASE WHEN Variable = 'Vs' THEN `Hour_0_18` END) AS Vs
        FROM `{table_name}`
        GROUP BY Date, Postcode

        UNION ALL

        SELECT STR_TO_DATE(CONCAT(Date, ' ', '18'), '%Y-%m-%d %H') AS DateTime, Postcode,
            MAX(CASE WHEN Variable = 'CC' THEN `Hour_0_21` END) AS CC,
            MAX(CASE WHEN Variable = 'CP' THEN `Hour_0_21` END) AS CP,
            MAX(CASE WHEN Variable = 'GST' THEN `Hour_0_21` END) AS GST,
            MAX(CASE WHEN Variable = 'HiT' THEN `Hour_0_21` END) AS HiT,
            MAX(CASE WHEN Variable = 'HL' THEN `Hour_0_21` END) AS HL,
            MAX(CASE WHEN Variable = 'LoT' THEN `Hour_0_21` END) AS LoT,
            MAX(CASE WHEN Variable = 'MWD' THEN `Hour_0_21` END) AS MWD,
            MAX(CASE WHEN Variable = 'MWS' THEN `Hour_0_21` END) AS MWS,
            MAX(CASE WHEN Variable = 'NCP' THEN `Hour_0_21` END) AS NCP,
            MAX(CASE WHEN Variable = 'SD' THEN `Hour_0_21` END) AS SD,
            MAX(CASE WHEN Variable = 'SD_P' THEN `Hour_0_21` END) AS SD_P,
            MAX(CASE WHEN Variable = 'SwD' THEN `Hour_0_21` END) AS SwD,
            MAX(CASE WHEN Variable = 'SwF' THEN `Hour_0_21` END) AS SwF,
            MAX(CASE WHEN Variable = 'T' THEN `Hour_0_21` END) AS T,
            MAX(CASE WHEN Variable = 'TP' THEN `Hour_0_21` END) AS TP,
            MAX(CASE WHEN Variable = 'Vs' THEN `Hour_0_21` END) AS Vs
        FROM `{table_name}`
        GROUP BY Date, Postcode

        UNION ALL

        SELECT STR_TO_DATE(CONCAT(Date, ' ', '21'), '%Y-%m-%d %H') AS DateTime, Postcode,
            MAX(CASE WHEN Variable = 'CC' THEN `Hour_0_24` END) AS CC,
            MAX(CASE WHEN Variable = 'CP' THEN `Hour_0_24` END) AS CP,
            MAX(CASE WHEN Variable = 'GST' THEN `Hour_0_24` END) AS GST,
            MAX(CASE WHEN Variable = 'HiT' THEN `Hour_0_24` END) AS HiT,
            MAX(CASE WHEN Variable = 'HL' THEN `Hour_0_24` END) AS HL,
            MAX(CASE WHEN Variable = 'LoT' THEN `Hour_0_24` END) AS LoT,
            MAX(CASE WHEN Variable = 'MWD' THEN `Hour_0_24` END) AS MWD,
            MAX(CASE WHEN Variable = 'MWS' THEN `Hour_0_24` END) AS MWS,
            MAX(CASE WHEN Variable = 'NCP' THEN `Hour_0_24` END) AS NCP,
            MAX(CASE WHEN Variable = 'SD' THEN `Hour_0_24` END) AS SD,
            MAX(CASE WHEN Variable = 'SD_P' THEN `Hour_0_24` END) AS SD_P,
            MAX(CASE WHEN Variable = 'SwD' THEN `Hour_0_24` END) AS SwD,
            MAX(CASE WHEN Variable = 'SwF' THEN `Hour_0_24` END) AS SwF,
            MAX(CASE WHEN Variable = 'T' THEN `Hour_0_24` END) AS T,
            MAX(CASE WHEN Variable = 'TP' THEN `Hour_0_24` END) AS TP,
            MAX(CASE WHEN Variable = 'Vs' THEN `Hour_0_24` END) AS Vs
        FROM `{table_name}`
        GROUP BY Date, Postcode
    """
    logging.debug(f'GFS Query: {query}')
    return query

