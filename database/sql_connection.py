import pyodbc
import configparser

import constants


def get_sql_connection():
    """
    This method simply returns the sql connection based on the DB type and the connection settings
    defined in the db.conf
    :return: connection
    """

    # Reading the Database configurations
    db_config = configparser.ConfigParser()
    db_config.read(constants.ROOT_DIR + constants.DB_CONFIG)
    db_type = db_config['SYSTEM']['db_type']
    server = db_config[db_type]['server']
    database = db_config[db_type]['database']
    # driver = db_config[db_type]['driver']
    driver = 'ODBC Driver 18 for SQL Server'
    # TODO: make the user and PWD also configurable in the conf file
    dsn = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID=sa;PWD=Sql123456;'
    dsn += 'TrustServerCertificate=Yes;'
    return pyodbc.connect(dsn)


def close_sql_connection(connection):
    """
    Take care of the closing process of the SQL connection
    :param connection: sql_connection
    :return: operation status
    """
    return connection.close()
