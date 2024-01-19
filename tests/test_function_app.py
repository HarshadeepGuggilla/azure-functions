import os
import unittest
import azure.functions as func
from azure.functions import HttpRequest
from azure.storage.blob import BlobServiceClient
from function_app import http_trigger, get_rolling_five_days, get_total_data


class TestFunctionApp(unittest.TestCase):

    def setUp(self):
        try:
            os.environ['AZURE_STORAGE_CONNECTION_STRING'] = 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;'
            
            blob_service_client = BlobServiceClient.from_connection_string(os.environ.get("AZURE_STORAGE_CONNECTION_STRING"))

            # Sample Covid-19 data kept in container 'sample-workitems'
            container_name = "sample-workitems"
            blob_name = "data.csv"

            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            
            blob_content = blob_client.download_blob().readall()

            self.input_blob=func.blob.InputStream(data=blob_content)
            
        except Exception as e:
            print(f"An error occurred: {e}")
            raise  #raise the exception to see the full traceback

    # Test case for invalid end point scenario
    def test_http_trigger_invalid_end_point(self):

        invalidendpoint = HttpRequest(
            method='GET',
            url='/api/http_trigger',
            body=None,
            route_params={ 
            "endpoint": "INVALID-END-POINT",
            "countryterritoryCode": "AUT",
            }
        )
        
        func_call = http_trigger.build().get_user_function()
        response=func_call(invalidendpoint, self.input_blob)
        assert response.status_code ==  400, "Failure: Status code is not 400"
        print("Success: Status code is 400")

    # Test case for empty end point scenario
    def test_http_trigger_empty_end_point(self):

        invalidendpoint = HttpRequest(
            method='GET',
            url='/api/http_trigger',
            body=None,
            route_params={ 
            "endpoint": "",
            "countryterritoryCode": "AUT",
            }
        )
        
        func_call = http_trigger.build().get_user_function()
        response=func_call(invalidendpoint, self.input_blob)
        assert response.status_code ==  400, "Failure: Status code is not 400"
        print("Success: Status code is 400")

    # Test case for rolling five days end point with valid country
    def test_http_trigger_rolling_five_days_valid_endpoint_country(self):

        rolling_five_days_valid_cntry = HttpRequest(
            method='GET',
            url='/api/http_trigger',
            body=None,
            route_params={ 
            "endpoint": "rolling-five-days",
            "countryterritoryCode": "AUT",
            }
        )
        
        func_call = http_trigger.build().get_user_function()
        response=func_call(rolling_five_days_valid_cntry, self.input_blob)
        assert response.status_code ==  200, "Failure: Status code is not 200"
        print("Success: Status code is 200")


    # Test case for rolling five days end point with invalid country
    def test_http_trigger_rolling_five_days_invalid_country(self):

        rolling_five_days_invalid_cntry = HttpRequest(
            method='GET',
            url='/api/http_trigger',
            body=None,
            route_params={ 
            "endpoint": "rolling-five-days",
            "countryterritoryCode": "IND",
            }
        )
        
        func_call = http_trigger.build().get_user_function()
        response=func_call(rolling_five_days_invalid_cntry, self.input_blob)
        assert response.status_code ==  404, "Failure: Status code is not 404"
        print("Success: Status code is 404")

    # Test case for total-data endpoint by passing country
    def test_http_trigger_total_data_endpoint_cntry(self):

        total_data_end_point_with_country = HttpRequest(
            method='GET',
            url='/api/http_trigger',
            body=None,
            route_params={ 
            "endpoint": "total-data",
            "countryterritoryCode": "AUT",
            }
        )
        
        func_call = http_trigger.build().get_user_function()
        response=func_call(total_data_end_point_with_country, self.input_blob)
        assert response.status_code ==  200, "Failure: Status code is not 200"
        print("Success: Status code is 200")

    # Test case for total-data endpoint by not passing country
    def test_http_trigger_total_data_endpoint_nocntry(self):

        total_data_end_point_with_country = HttpRequest(
            method='GET',
            url='/api/http_trigger',
            body=None,
            route_params={ 
            "endpoint": "total-data",
            "countryterritoryCode": "",
            }
        )
        
        func_call = http_trigger.build().get_user_function()
        response=func_call(total_data_end_point_with_country, self.input_blob)
        assert response.status_code ==  200, "Failure: Status code is not 200"
        print("Success: Status code is 200")

    ################MORE TEST CASES CAN BE ADDED#############


if __name__ == '__main__':
    unittest.main()