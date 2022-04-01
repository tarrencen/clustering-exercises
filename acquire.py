import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from env import get_db_url
from pydataset import data
import os

def show_codeup_dbs():
    url = get_db_url('employees')
    codeup_dbs = pd.read_sql('SHOW DATABASES', url)
    print('List of Codeup DBs:\n')
    return codeup_dbs


def get_props_2017():
    '''
    Returns a DataFrame composed of selected data from the properties_2017 table in the zillow database on
    Codeup's SQL server
    '''
    filename = 'props_2017.csv'
    if os.path.exists(filename):
        print('Reading from CSV file...')
        return pd.read_csv(filename)
    query = """
    SELECT 
    bedroomcnt, 
    bathroomcnt, 
    roomcnt,
    numberofstories,
    fireplaceflag,
    poolcnt, 
    buildingqualitytypeid, 
    calculatedfinishedsquarefeet, 
    lotsizesquarefeet,
    latitude,
    longitude,
    structuretaxvaluedollarcnt,
    landtaxvaluedollarcnt,
    taxvaluedollarcnt, 
    yearbuilt, 
    taxamount, 
    fips, 
    logerror 
    FROM properties_2017 prop
    JOIN predictions_2017 pred ON pred.id = prop.id
    JOIN propertylandusetype land ON land.propertylandusetypeid = prop.propertylandusetypeid
    WHERE (
    prop.latitude IS NOT NULL
    AND
    prop.longitude IS NOT NULL
    AND 
    pred.transactiondate LIKE '2017%%'
    );
    """
    print('Getting a fresh copy from SQL database...')
    url = get_db_url('zillow')
    props_2017 = pd.read_sql(query, url)
    print('Copying to CSV...')
    props_2017.to_csv(filename, index=False)
    return props_2017

def zillow_cluster_f():
    '''
    Returns a dataframe from a SQL query of the Zillow data on Codeup's MySQL server
    '''
    filename = 'zillow_cluster_f.csv'
    if os.path.exists(filename):
        print('Reading from CSV file...')
        return pd.read_csv(filename)
    query = '''
    SELECT *
    FROM properties_2017 prop
    LEFT JOIN airconditioningtype ac ON ac.airconditioningtypeid = prop.airconditioningtypeid
    LEFT JOIN architecturalstyletype arch ON arch.architecturalstyletypeid = prop.architecturalstyletypeid
    LEFT JOIN buildingclasstype build ON build.buildingclasstypeid = prop.buildingclasstypeid
    LEFT JOIN heatingorsystemtype heat ON heat.heatingorsystemtypeid = prop.heatingorsystemtypeid
    LEFT JOIN predictions_2017 pred ON pred.id = prop.id
    LEFT JOIN propertylandusetype land on land.propertylandusetypeid = prop.propertylandusetypeid
    LEFT JOIN storytype st ON st.storytypeid = prop.storytypeid
    LEFT JOIN typeconstructiontype con ON con.typeconstructiontypeid = prop.typeconstructiontypeid
    WHERE (
    prop.latitude IS NOT NULL
    AND
    prop.longitude IS NOT NULL
    AND
    pred.transactiondate LIKE '2017%%'
    )'''
    print('Getting a fresh copy from SQL database...')
    url = get_db_url('zillow')
    zillow_cluster_f = pd.read_sql(query, url)
    print('Copying to CSV...')
    zillow_cluster_f.to_csv(filename, index=False)
    return zillow_cluster_f


