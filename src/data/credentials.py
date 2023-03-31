import configparser
import os

def get_file():
    base_dir = os.path.abspath(os.getcwd())
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

if __name__ == '__main__':
    base_dir, config_file = get_file()
    host, database, user, password = get_configs(file=config_file, section='test')
