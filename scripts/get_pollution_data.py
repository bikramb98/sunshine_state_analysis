import requests
import time
import pandas as pd
import numpy as np
import os
from pandas import json_normalize

def get_county_codes():

    '''Get county codes for different counties in California'''
    
    county_codes_api_url = "https://aqs.epa.gov/data/api/list/countiesByState?email=test@aqs.api&key=test&state=06"
    county_code_response = requests.get(county_codes_api_url)

    county_codes = json_normalize(county_code_response.json()['Data'])
    county_codes_list = county_codes['code']
    
    return county_codes_list

def generate_date_list(start_year,end_year):

    '''Generate the date list on the format required by the API'''
    
    start_date_list = []
    end_date_list = []
    
    for year in range(start_year, end_year + 1):
        start_date_list.append(int(f"{year}0101"))
        end_date_list.append(int(f"{year}1231"))
        
    return start_date_list, end_date_list

def fetch_air_quality_data(county_codes_list, start_year, end_year):

    '''Function to get pollution data onto a list. The email and key will be read from the environment variables
    The function waits for 10 secs before executing again in order to comply with API usage limits'''
    
    email = os.getenv('API_EMAIL')
    key = os.getenv('API_KEY')
    
    start_date_list, end_date_list = generate_date_list(start_year,end_year)
    
    data_list = []
        
    for county_code in county_codes_list:
        
        for start_date, end_date in zip(start_date_list, end_date_list):
            
            api_url = f"https://aqs.epa.gov/data/api/annualData/byCounty?email={email}&key={key}&param=42101&bdate={start_date}&edate={end_date}&state=06&county={county_code}"
            
            response = requests.get(api_url)

            try:
                data = json_normalize(response.json()['Data'])
            except:
                continue
            
            try:
                county = data['county'][0]
                arth_mean = np.mean(data['arithmetic_mean'])
                first_max_date = data['first_max_datetime'].max()
                second_max_date = data['second_max_datetime'].max()
                third_max_date = data['third_max_datetime'].max()
                year = pd.DatetimeIndex(data['third_max_datetime']).year[0]

                data_list.append([county, year, arth_mean, first_max_date, second_max_date, third_max_date])
                            
            except:
                continue
            
            time.sleep(10) #to comply with API usage
    
    return data_list

start_year = 2012
end_year = 2013

county_codes_list = get_county_codes()

data = fetch_air_quality_data(county_codes_list, start_year, end_year)

df_columns = ['County','Year','Arth_Mean','First_Max_Date','Second_Max_Date','Third_Max_Date']
co_df = pd.DataFrame(data, columns=df_columns)
co_df.to_csv("../datasets/pollution_data.csv") 