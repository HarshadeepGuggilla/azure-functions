• Go to:
https://www.ecdc.europa.eu/en/publications-data/data-daily-new-cases-covid-19-eueea-country
Here you will find the latest European Covid-19 case data.
Download the dataset in any format you would like.
Using azure functions, set up a simple API which runs locally which can be called to 
search the dataset.

• Implement an endpoint: /rolling-five-days/{countryterritoryCode}which returns the past 5 days of 
data given a country code. You can simply consider the latest 5 days of data, since it is no longer 
continuously updated. Return the data in application-jsonformat (come up with a suitable return 
model for the data).


• Implement an endpoint: /total-data which returns the total number of cases for each country (come 
up with a suitable return model for the data).


Consider a strategy for testing the API --- python unittest framework is used.
Consider a storing solution for the dataset --- Idea is store in blob/ADLS but now storing in local storage emulator for demo purpose.


*****************THIS IS ONE OF THE MANY POSSIBLE SOLUTIONS, SAME THING CAN BE ACHIEVED USING DIFFERENT SET OF TOOLS AS WELL*****************
