from importers.imports import *

scheme_details_url = "https://mfapi.advisorkhoj.com/getSchemeInfo?key=7a041409-cf1a-4cae-9083-ebbf75c4a668"
all_mf_api_url = "https://mfapi.advisorkhoj.com/getAllMutualFundSchemesbyAmcAndCategory?key=7a041409-cf1a-4cae-9083-ebbf75c4a668&amc=All&category=All"

def all_scheme_urls():
    all_mf_response = requests.get(all_mf_api_url)
    if all_mf_response.status_code == 200:
        all_mfs = all_mf_response.json()
    else:
        print(f"API request failed with status code: {all_mf_response.status_code}")
    # Parameter name to be used in the input data
    parameter_name = "scheme"

    # List of values for the parameter
    input_values = all_mfs["list"]

    # Create input data by pairing the parameter name with values
    input_data_list = [{parameter_name: value} for value in input_values]
    return input_data_list

# Define the output file
output_file = "advisorkhoj/logs/all_apis_data.txt"

# Function to call the API with input data and return the response data
def call_api(input_data):
    try:
        response = requests.get(scheme_details_url, params=input_data)
        response_data = response.json()
        return response_data
    except Exception as e:
        print(f"Error while calling the API: {str(e)}")
        return None

def concurrent_api_call():
    # Use a ThreadPoolExecutor to call APIs in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        api_responses = list(executor.map(call_api, all_scheme_urls()))

    # Remove None values (responses with errors)
    api_responses = [response for response in api_responses if response is not None]

    # Save the API responses to a file
    with open(output_file, 'w') as file:
        json.dump(api_responses, file)

    print(f"API responses saved to {output_file}")



def read_api_data():
    concurrent_api_call()
    try:
        with open(output_file, 'r') as file:
            api_responses = json.load(file)
        return api_responses
    except FileNotFoundError:
        print(f"File {output_file} not found.")
    except json.JSONDecodeError as e:
        print(f"Error while decoding JSON: {str(e)}")
