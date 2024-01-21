import csv
import json
import logging
import datetime
import pandas as pd
import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.blob_input(arg_name="inputBlob",
                path="sample-workitems/data.csv",
                connection="AzureWebJobsStorage")
@app.route(route="http_trigger/{endpoint?}/{countryterritoryCode?}")
def http_trigger(req: func.HttpRequest,  inputBlob: func.InputStream) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
        # Extract endpoint and country territory code from route parameters
        endpoint = req.route_params.get('endpoint')
        country_code = req.route_params.get('countryterritoryCode')

        if not endpoint:
            return func.HttpResponse("Endpoint not provided. Please specify the endpoint in the URL.", status_code=400)

        if endpoint == 'rolling-five-days':
            if not country_code:
                return func.HttpResponse("Country code not provided. Please specify the country code in the URL.", status_code=400)
            
            return get_rolling_five_days(inputBlob, country_code)

        elif endpoint == 'total-data':
            return get_total_data(inputBlob)

        else:
            return func.HttpResponse("Invalid endpoint. Supported endpoints are 'rolling-five-days' and 'total-data'.", status_code=400)

    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

    

# Logic for endpoint /rolling-five-days/country_code - returns the past 5 days of data given a country code.
def get_rolling_five_days(inputBlob, country_code):
    try:
        current_date = datetime.datetime.now()
        logging.info(f'Started execution for endpoint rolling-five-days and {country_code}'+str(current_date))

        df = read_preprocess_data(inputBlob) # Data Reading and pre-processing step

        # Filter past five days data for the given country code
        latest_date = df.loc[df['countryterritoryCode'] == country_code, 'dateRep'].max()
        last_five_days_data = df[
            (df['countryterritoryCode'] == country_code) &
            (df['dateRep'] >= (latest_date - pd.DateOffset(days=4)))  # Adjusted to consider only the latest five days
        ]

        if last_five_days_data.empty:
            return func.HttpResponse(f"No data found for country code {country_code} in the last five days.", status_code=404)

        # Extract and structure the relevant data for the response
        response_data = [
            {
                "dateRep": row['dateRep'].strftime("%d/%m/%Y"),  
                "cases": row['cases'],
                "deaths": row['deaths']
            }
            for _, row in last_five_days_data.iterrows()
        ]


        # Reconciliation record with start date, end date, total cases, total deaths, and total records
        start_date = (latest_date - pd.DateOffset(days=4)).strftime("%d/%m/%Y")
        end_date = latest_date.strftime("%d/%m/%Y")
        reconciliation_record = {
            "Start Date": start_date,
            "End Date": end_date,
            "Total Cases": int(last_five_days_data['cases'].sum()),
            "Total Deaths": int(last_five_days_data['deaths'].sum()),
            "Total Records": len(last_five_days_data)
        }

       # Construct the final response document
        response_document = {
            "SourceDataset": "5-Day Covid-19 Report",
            "SourceSystem": "ECDC(European Centre for Disease Prevention and Control)",
            "SourcePointofContact": "Digital, Data & IT Team",
            "SourceDataRefreshDate": "To be added",
            "SourceDataRefreshFrequency": "Weekly Twice",
            "HouseKeeping": "More house keeping columns can also be added", 
            "GeoId": last_five_days_data.iloc[0]['geoId'],
            "CountryTerritoryCode": last_five_days_data.iloc[0]['countryterritoryCode'],
            "Country": last_five_days_data.iloc[0]['countriesAndTerritories'],
            "Continent": last_five_days_data.iloc[0]['continentExp'],
            "ReconciliationRecord": reconciliation_record,
            "Records": response_data
        }
        logging.info(f'Completed execution for endpoint rolling_five_days and {country_code}'+str(current_date))
        return func.HttpResponse(
            json.dumps(response_document, indent=2),
            mimetype="application/json",
            status_code=200
        )
            
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
    
    

# Logic for total-data end point - returns the total number of cases for each country
def get_total_data(inputBlob):
    try:
        current_date = datetime.datetime.now()
        logging.info(f'Started execution for endpoint total-data'+str(current_date))
        
        df = read_preprocess_data(inputBlob) # Data Reading and pre-processing step

        # Group by countryterritoryCode and calculate total cases and deaths
        total_data = df.groupby('countryterritoryCode').agg({
            'cases': 'sum',
            'deaths': 'sum'
        }).reset_index()

        # Extract and structure the relevant data for the response
        response_data = [
            {
                "countryterritoryCode": row['countryterritoryCode'],
                "totalCases": row['cases'],
                "totalDeaths": row['deaths']
            }
            for _, row in total_data.iterrows()
        ]

        # Reconciliation record for total data
        reconciliation_record = {
            "Total Cases": total_data['cases'].sum(),
            "Total Deaths": total_data['deaths'].sum(),
            "Total Records": len(total_data),
        }
        
        # Construct the final response document
        response_document = {
            "SourceDataset": "Aggregated Covid-19 Report",
            "SourceSystem": "ECDC(European Centre for Disease Prevention and Control)",
            "SourcePointofContact": "Digital, Data & IT Team",
            "SourceDataRefreshDate": "To be added",
            "SourceDataRefreshFrequency": "Weekly Twice",
            "HouseKeeping": "More house keeping columns can also be added", 
            "ReconciliationRecord": reconciliation_record,
            "Records": response_data
        }
        logging.info(f'Completed execution for endpoint total_data'+str(current_date))
        return func.HttpResponse(
            json.dumps(response_document, indent=2),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

# Data Pre-processing and cleaning up
def read_preprocess_data(inputBlob):

    # Read CSV to pandas dataframe
    df = pd.read_csv(inputBlob, parse_dates=['dateRep'], dayfirst=True)

    if df.empty:
            return func.HttpResponse("No data found.", status_code=404)
    
    # Fill 0 in cases and deaths columns for rows with blanks
    df['cases'].fillna(0, inplace=True)
    df['deaths'].fillna(0, inplace=True)
    
    # Remove rows with negative numbers in cases or deaths columns
    df = df[(df['cases'] >= 0) & (df['deaths'] >= 0)]

    # Convert dateRep column to datetime format
    df['dateRep'] = pd.to_datetime(df['dateRep'], format='%d/%m/%Y', errors='coerce')

    # Drop duplicate records if any
    df.drop_duplicates(inplace=True)

    return df
