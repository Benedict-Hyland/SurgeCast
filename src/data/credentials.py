import configparser
import os

def get_mysql_configs():
    script_dir = os.path.dirname(os.path.abspath(__file__))

    base_dir = os.path.abspath(os.path.join(script_dir, '../../'))

    config = configparser.ConfigParser()
    config.read(os.path.join(base_dir, '/config/database_credentials.ini'))

    host = config.get('mysql', 'host')
    database = config.get('mysql', 'database')
    user = config.get('mysql', 'user')
    password = config.get('mysql', 'password')

    return host, database, user, password

if __name__ == '__main__':
    host, database, user, password = get_mysql_configs()
