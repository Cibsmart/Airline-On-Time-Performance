import os
import bz2
import time
import sqlite3
import numpy as np
import pandas as pd


db_file_path = 'datasets/data.db'


def create_tables(conn=sqlite3.connect(db_file_path)):

    """Creates flights, airports, carriers and plane_data tables
    
    Parameters
    ----------
    conn : sqlite3.Connetion, optional
        Connection to sqlite database
    """
    
    cursor = conn.cursor()

    # Create Flights Table
    cursor.execute('DROP TABLE IF EXISTS flights')

    query = """
        CREATE TABLE flights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER,
            month INTEGER, 
            day_of_month INTEGER,
            day_of_week INTEGER, 
            dep_time INTEGER,
            crs_dep_time INTEGER, 
            arr_time INTEGER,
            crs_arr_time INTEGER,
            unique_carrier TEXT,
            flight_num INTEGER,
            tail_num TEXT,
            actual_elapsed_time INTEGER, 
            crs_elapsed_time INTEGER,
            air_time INTEGER, 
            arr_delay INTEGER,
            dep_delay INTEGER, 
            origin TEXT,
            dest TEXT, 
            distance INTEGER, 
            taxi_in INTEGER,
            taxi_out INTEGER, 
            cancelled INTEGER,
            cancellation_code TEXT, 
            diverted INTEGER,
            carrier_delay INTEGER, 
            weather_delay INTEGER,
            nas_delay INTEGER, 
            security_delay INTEGER,
            late_aircraft_delay INTEGER)
    """

    cursor.execute(query)



    # Create Airports Table
    cursor.execute('DROP TABLE IF EXISTS airports')

    query = """
        CREATE TABLE airports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            iata TEXT,
            airport TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            lat NUMERIC,
            long NUMERIC)
    """
    cursor.execute(query)



    # Create Carriers Data Table
    cursor.execute('DROP TABLE IF EXISTS carriers')

    query = """
        CREATE TABLE carriers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT,
            description TEXT)
    """
    cursor.execute(query)



    # Create Plane Data Table
    cursor.execute('DROP TABLE IF EXISTS plane_data')

    query = """
        CREATE TABLE plane_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tailnum TEXT,
            type TEXT,
            manufacturer TEXT,
            issue_date TEXT,
            model TEXT,
            status TEXT,
            aircraft_type TEXT,
            engine_type TEXT,
            year INTEGER)
    """
    cursor.execute(query)
    
    conn.close()



def read_data(conn=sqlite3.connect(db_file_path), 
              path='datasets/data', 
              chunksize=5000000, 
              compression='bz2', 
              encoding='ISO-8859-1'):
    
    """Read Compressed CSV Files in a Path into a Database
    
    Parameters
    ----------
    conn : sqlite3.Connetion, optional
        Connection to sqlite database
    path : str, optional
        Directory path to be read
    chunksize : int
        Size of chunks to read a time
    compression : str
        Comprresion type
    encoding : str, optional
        Encoding type
    """
    
    cursor = conn.cursor()

    files = sorted(os.listdir(path))[:-1]

    chunksize=5000000 
    compression='bz2'
    encoding='ISO-8859-1'

    names = {
        'Year': 'year',
        'Month': 'month',
        'DayofMonth': 'day_of_month',
        'DayOfWeek': 'day_of_week',
        'DepTime': 'dep_time',
        'CRSDepTime': 'crs_dep_time',
        'ArrTime': 'arr_time',
        'CRSArrTime': 'crs_arr_time',
        'UniqueCarrier': 'unique_carrier',
        'FlightNum': 'flight_num',
        'TailNum': 'tail_num',
        'ActualElapsedTime': 'actual_elapsed_time',
        'CRSElapsedTime': 'crs_elapsed_time',
        'AirTime': 'air_time',
        'ArrDelay': 'arr_delay',
        'DepDelay': 'dep_delay',
        'Origin': 'origin',
        'Dest': 'dest',
        'Distance': 'distance',
        'TaxiIn': 'taxi_in',
        'TaxiOut': 'taxi_out',
        'Cancelled': 'cancelled',
        'CancellationCode': 'cancellation_code',
        'Civerted': 'diverted',
        'CarrierDelay': 'carrier_delay',
        'WeatherDelay': 'weather_delay',
        'NASDelay': 'nas_delay',
        'SecurityDelay': 'security_delay',
        'LateAircraftDelay': 'late_aircraft_delay'
    }

    for file in files:

        if file.endswith('bz2'):
            pass
            for chunk in pd.read_csv('{}/{}'.format(path, file), 
                                     chunksize=chunksize, 
                                     encoding=encoding, 
                                     compression=compression):

                chunk.rename(columns=names, inplace=True)

                chunk.to_sql(name='flights', con=conn, if_exists='append', index=False)
        else:
            table_name = file.replace('.csv', '').replace('-', '_')
            df = pd.read_csv('{}/{}'.format(path, file))
            df.to_sql(name=table_name, con=conn, if_exists='append', index=False)
        
    conn.close()



def add_date_column(conn=sqlite3.connect(db_file_path)):
    
    """Add date column to the flights table and set an index on that column
    
    Parameters
    ----------
    conn : sqlite3.Connetion, optional
        Connection to sqlite database
    """
    
    cursor = conn.cursor()
    
    # Add date column to the flights table
    query = """ALTER TABLE flights ADD COLUMN date datetime;"""

    cursor.execute(query)

    # Compute date column with year, month and day_of_month fields
    query = """UPDATE flights SET date = year || '-' || month || '-' || day_of_month"""

    cursor.execute(query)

    # Add index on the date column to improve performance
    query = """CREATE INDEX date ON flights(date);"""

    cursor.execute(query)
    
    conn.commit()
    
    conn.close()


def query_to_df(query, 
                conn=sqlite3.connect(db_file_path), 
                index_col=None, 
                parse_dates=None,
                chunksize=5000000,
                optimize=False):
    
    """Creates flights, airports, carriers and plane_data tables
    
    Parameters
    ----------
    query : str
        SQL query to be executed
    conn : sqlite3.Connetion, optional
        Connection to sqlite database
    index_col : str, optional
    parse_date : list, optional
    chunksize : int, optional
        
    Returns
    -------
    df : pandas DataFrame
    """
    start = time.time()
    
    cursor = conn.cursor()
    
    df = pd.DataFrame()
    
    for chunk in pd.read_sql(query, 
                             conn, 
                             index_col=index_col, 
                             parse_dates=parse_dates, 
                             chunksize=chunksize):
        
        df = pd.concat([df, chunk])
        
    if optimize:
        df = optimize_dataframe(df)
    
    end = time.time()
    
    print('Total Run Time: {:0.00f} minutes'.format((end - start) / 60.0))
        
    return df



def optimize_dataframe(df):
    
    """Update column datatypes
    
    Parameters
    ----------
    df : pandas DataFrame
        Dataframe containing column to be updated
        
    Returns
    -------
    df : pandas DataFrame
    """
    
    dtypes = {
        'year': np.int16,
        'month': np.int8,
        'day_of_month': np.int8,
        'day_of_week': np.int8,
        'arr_delay': np.int16,
        'dep_delay': np.int16,
        'unique_carrier': 'category',
        'origin': 'category',
        'dest': 'category',
        'tailnum': 'category',
        'cancelled': bool,
        'diverted': bool
    }
    
    print('Memory usage before optimization {:.2f} GB'.format(df.memory_usage(deep=True).sum() / 1024**3))
    
    for col in df.columns:
        if col in dtypes:
            df[col] = df[col].astype(dtypes[col])
        
    print('Memory usage after optimization {:.2f} GB'.format(df.memory_usage(deep=True).sum() / 1024**3))
    return df