import azure.functions as func
import logging
import json
import csv
import datetime
import pandas as pd

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.blob_input(arg_name="inputBlob",
                path="sample-workitems/data.csv",
                connection="AzureWebJobsStorage")
@app.route(route="http_trigger/{endpoint}/{countryterritoryCode?}")
def http_trigger(req: func.HttpRequest,  inputBlob: func.InputStream) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
        # Extract endpoint and country code from route parameters
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

    

def get_rolling_five_days(inputBlob, country_code):
    try:
        # Read CSV into a Pandas DataFrame, parse 'dateRep' column in the correct date format
        df = pd.read_csv(inputBlob, parse_dates=['dateRep'], dayfirst=True)
        
        if df.empty:
            return func.HttpResponse("No data found.", status_code=404)

        ######DATA PRE-PROCESSING LOGIC FOR PANDAS DATAFRAME CAN BE ADDED HERE########

        # Filter past five days data for the given country code
        current_date = datetime.datetime.now()
        start_date = (current_date - datetime.timedelta(days=5)).strftime("%d/%m/%Y")
        end_date = current_date.strftime("%d/%m/%Y")
        last_five_days_data = df[
            (df['countryterritoryCode'] == country_code) &
            (df['dateRep'] >= (current_date - datetime.timedelta(days=1000)))
        ]

        if last_five_days_data.empty:
            return func.HttpResponse(f"No data found for country code {country_code} in the last five days.", status_code=404)

        # Extract and structure the relevant data for the response
        response_data = [
            {
                "dateRep": row['dateRep'].strftime("%d/%m/%Y"),  
                "day": row['day'],
                "month": row['month'],
                "year": row['year'],
                "cases": row['cases'],
                "deaths": row['deaths']
            }
            for _, row in last_five_days_data.iterrows()
        ]


        # Reconciliation record with start date, end date, total cases, total deaths, and total records
        reconciliation_record = {
            "startDate": start_date,
            "endDate": end_date,
            "totalCases": int(last_five_days_data['cases'].sum()),
            "totalDeaths": int(last_five_days_data['deaths'].sum()),
            "totalRecords": len(last_five_days_data),
        }

       # Construct the final response document
        response_document = {
            "Source Dataset": "5-Day Covid-19 Report",
            "Source System": "ECDC(European Centre for Disease Prevention and Control)",
            "Last Source Data refresh date": "To be added",
            "Source Data update frequency": "Weekly Twice",
            "Source contact": "To be added",
            "House keeping": "More house keeping columns can also be added", 
            "geoId": last_five_days_data.iloc[0]['geoId'],
            "countryterritoryCode": last_five_days_data.iloc[0]['countryterritoryCode'],
            "country": last_five_days_data.iloc[0]['countriesAndTerritories'],
            "continent": last_five_days_data.iloc[0]['continentExp'],
            "Reconciliation Record": reconciliation_record,
            "records": response_data
        }

        return func.HttpResponse(
            json.dumps(response_document, indent=2),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
    


def get_total_data(inputBlob):
    try:
        # Read CSV into a Pandas DataFrame
        df = pd.read_csv(inputBlob)

        if df.empty:
            return func.HttpResponse("No data found.", status_code=404)

        # Group by countryterritoryCode and calculate total cases and deaths
        total_data = df.groupby('countryterritoryCode').agg({
            'cases': 'sum',
            'deaths': 'sum'
        }).reset_index()

        if total_data.empty:
            return func.HttpResponse("No total data found.", status_code=404)

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
            "totalCases": total_data['cases'].sum(),
            "totalDeaths": total_data['deaths'].sum(),
            "totalRecords": len(total_data),
        }
        
        # Construct the final response document
        response_document = {
            "Source Dataset": "Aggregated Covid-19 Report",
            "Source System": "ECDC(European Centre for Disease Prevention and Control)",
            "Last Source Data refresh date": "To be added",
            "Source Data update frequency": "Weekly Twice",
            "Source contact": "To be added",
            "House keeping": "More house keeping columns can also be added", 
            "Reconciliation Record": reconciliation_record,
            "records": response_data
        }

        return func.HttpResponse(
            json.dumps(response_document, indent=2),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
