import logging
import credentials
import queries
import pandas as pd
import concurrent.futures
import functools
import pandas as pd
from datetime import timedelta
import time

start_time = time.time()

# region Get the files so that credentials can be accessed
base_dir, config_file = credentials.get_file()

logging.basicConfig(filename=f'{base_dir}/logs/get_data/main.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info('Logger started')
# endregion

# region Get the credentials for both databases needed
logging.debug('Collecting test database credentials, if this fails, it is likely that the credential import has not been imported correctly')
test_host, test_database, test_user, test_password = credentials.get_configs(file=config_file, section='test')  # Test database is for the test data

logging.debug('Collecting gfs and net database credentials, if this fails, it is likely that the credential import has not been restarted')
gfs_host, gfs_database, gfs_user, gfs_password = credentials.get_configs(file=config_file, section='gfs')   # GFS database is for the weather data
net_host, net_database, net_user, net_password = credentials.get_configs(file=config_file, section='net')   # Net database is for the claims data

# endregion
logging.info('Successfully collected all database credentials')

def shape_gfs(data):
    melt = data.melt(id_vars=['Date', 'Variable', 'Postcode'], var_name='Hour', value_name='Value')
    melt['DateTime'] = pd.to_datetime(melt['Date']) + pd.to_timedelta(melt['Hour'].astype(int), unit='h')
    melt.drop(columns=['Date', 'Hour'], inplace=True)
    pivot = melt.pivot_table(index=["DateTime", "Postcode"], columns="Variable", values="Value").reset_index()
    return pivot

def collect():
    years = range(2021, 2023)  # Replace these values with the desired year range
    months = range(1, 13)

    name_generator = lambda y, m: f'Hindcast_day0_districts_{y}_{m:02d}'
    table_names = [name_generator(year, month) for year in years for month in months]
    logging.debug(f'Created table names: {table_names}')

    with concurrent.futures.ThreadPoolExecutor() as executor:
        gfs_queries = list(executor.map(queries.get_gfs_query, table_names))
        logging.debug(f'Created gfs queries: {gfs_queries}')
        partial_run_query = functools.partial(queries.run_query, gfs_host, gfs_database, gfs_user, gfs_password)
        logging.info(f'Starting to run queries')
        results = list(executor.map(partial_run_query, gfs_queries))
        logging.debug('Starting to shape data')
        pivots = list(executor.map(shape_gfs, results))
        logging.debug('Collecting the claims data')
    del gfs_queries, partial_run_query, results
    logging.info('Successfully collected all data')
    
    df = pd.concat(pivots, ignore_index=True)
    logging.info('Successfully concatenated all data')
    
    start_datetime = df['DateTime'].min()
    end_datetime = df['DateTime'].max() + timedelta(hours=3)
    
    # region Collecting the claims data
    logging.debug(f'Collecting claims data for {start_datetime} to {end_datetime}')
    net_query = queries.get_counts_query(start_date=start_datetime, end_date=end_datetime)
    logging.debug('Successfully collected claims query')
    claims = queries.run_query(host=net_host, database=net_database, user=net_user, password=net_password, query=net_query)
    logging.info('Successfully collected claims data')
    # endregion
    
    # region Group the claims by Postcode and DateTime and create a column called counts which is the number of claims for that postcode and datetime
    grouped_claims = claims.groupby(['DateTime', 'Postcode']).size().reset_index(name='counts')
    logging.debug('Successfully grouped claims data')
    # endregion

    logging.debug('Merging gfs_data and counts by Postcode and DateTime, if counts is null, set it to 0')
    weather_counts = df.merge(grouped_claims, how='left', on=['Postcode', 'DateTime']).fillna(0)
    logging.info(f'Successfully merged gfs data and claims data from {start_datetime} to {end_datetime}')
    
    
    return weather_counts

if __name__ == '__main__':
    weather_counts = collect()
    weather_counts.to_csv(f'{base_dir}/src/data/data.csv', index=False)
    logging.info(f'Successfully saved data.csv')
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f'The script started at {start_time} and ended at {end_time} and took {elapsed_time/60} minutes to run')
        
