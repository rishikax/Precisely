import os
import time
import json
import base64
import datetime
import requests
import threading
import pandas as pd
from tqdm import tqdm
from pandas import json_normalize


class demographicsDataPrecisely:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.auth_token = None
        self.token_expiry_time = None
        self.url = "https://api.cloud.precisely.com/data-graph/graphql"
        self.refresh_token()

    def generate_auth_token(self):
        url = "https://api.cloud.precisely.com/auth/v2/token"
        payload = 'grant_type=client_credentials&scope=default'
        auth_string = f"{self.client_id}:{self.client_secret}"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {encoded_auth}'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 200:
            return json.loads(response.text)['access_token']
        else:
            raise Exception(f"Token generation failed with status code {response.status_code}: {response.text}")

    def refresh_token(self):
        print("Generating new auth token...")
        self.auth_token = self.generate_auth_token()
        self.token_expiry_time = datetime.datetime.now() + datetime.timedelta(minutes=59)
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.auth_token}"
        }
        print("New token generated and set.")

    def check_token_expiry(self):
        if datetime.datetime.now() >= self.token_expiry_time:
            print("Auth token expired. Regenerating...")
            time.sleep(10)  # Sleep for 10 seconds
            self.refresh_token()
            print("Token refreshed. Resuming operations...")

    def process_dataframe(self, df):
        results = []
        for index, row in tqdm(df.iterrows(), total=len(df), desc="Processing Precisely IDs"):
            self.check_token_expiry()
            
            precisely_id = row['PBKEY']

            psyte_query = self.generate_psyteGeodemographics_query(precisely_id)
            psyte_response = self.get_response(psyte_query)
            
            coastal_risk_query = self.generate_coastalRisk_query(precisely_id)
            coastal_risk_response = self.get_response(coastal_risk_query)
            
            flood_risk_query = self.generate_floodRisk_query(precisely_id)
            flood_risk_response = self.get_response(flood_risk_query)
            
            results.append({
                "precisely_id": precisely_id,
                "psyte_response": psyte_response,
                "coastal_risk_response": coastal_risk_response,
                "flood_risk_response": flood_risk_response
            })
        return results

    def generate_psyteGeodemographics_query(self, precisely_id):
        # GraphQL query for psyteGeodemographics (same as before)
        return f"""
        query addressByPreciselyID {{
          getById(id: "{precisely_id}", queryType: PRECISELY_ID) {{
            addresses {{
              data {{
                psyteGeodemographics {{
                  data {{
                    PSYTECategoryCode
                    PSYTEGroupCode
                    PSYTESegmentCode {{
                      description
                    }}
                    censusBlock
                    censusBlockGroup
                    censusBlockPopulation
                    censusBlockHouseholds
                    householdIncomeVariable {{
                      value
                      description
                    }}
                    propertyValueVariable {{
                      value
                      description
                    }}
                    propertyTenureVariable {{
                      value
                      description
                    }}
                    propertyTypeVariable {{
                      value
                      description
                    }}
                    urbanRuralVariable {{
                      value
                      description
                    }}
                  }}
                }}
              }}
            }}
          }}
        }}
        """

    def generate_coastalRisk_query(self, precisely_id):
        # GraphQL query for coastalRisk (same as before)
        return f"""
        query coastalRisk {{
            getById(id: "{precisely_id}", queryType: PRECISELY_ID) {{
                addresses {{
                    data {{
                        coastalRisk {{
                            data {{
                                preciselyID
                                waterbodyName
                                nearestWaterbodyCounty
                                nearestWaterbodyState
                                nearestWaterbodyType {{
                                    value
                                    description
                                }}
                                nearestWaterbodyAdjacentName
                                nearestWaterbodyAdjacentType
                                distanceToNearestCoastFeet
                                windpoolDescription
                            }}
                        }}
                    }}
                }}
            }}
        }}
        """

    def generate_floodRisk_query(self, precisely_id):
        # GraphQL query for floodRisk (same as before)
        return f"""
        query floodRisk {{
          getById(id: "{precisely_id}", queryType: PRECISELY_ID) {{
            addresses {{
              data {{
                floodRisk {{
                  data {{
                    preciselyID
                    floodID
                    femaMapPanelIdentifier
                    floodZoneMapType
                    stateFIPS
                    floodZoneBaseFloodElevationFeet
                    floodZone
                    additionalInformation
                    baseFloodElevationFeet
                    communityNumber
                    communityStatus
                    mapEffectiveDate
                    letterOfMapRevisionDate
                    letterOfMapRevisionCaseNumber
                    floodHazardBoundaryMapInitialDate
                    floodInsuranceRateMapInitialDate
                    addressLocationElevationFeet
                    year100FloodZoneDistanceFeet
                    year500FloodZoneDistanceFeet
                    elevationProfileToClosestWaterbodyFeet
                    distanceToNearestWaterbodyFeet
                    nameOfNearestWaterbody
                  }}
                }}
              }}
            }}
          }}
        }}
        """

    def get_response(self, query):
        payload = {"query": query}
        response = requests.post(self.url, json=payload, headers=self.headers)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Query failed with status code {response.status_code}: {response.text}")
        
class dataProcessorForDemographics:
    def __init__(self, results):
        self.results = results
        self.combined_df = None

    def extract_data(self, result):
        precisely_id = result['precisely_id']
        psyte_data = result.get('psyte_response', {}).get('data', {}).get('getById', {}).get('addresses', {}).get('data', [{}])[0].get('psyteGeodemographics')
        if psyte_data is not None:
            psyte_data = psyte_data.get('data', [{}])[0]

        coastal_data = {}
        try:
            coastal_data = result.get('coastal_risk_response', {}).get('data', {}).get('getById', {}).get('addresses', {}).get('data', [])
            if coastal_data and isinstance(coastal_data, list) and len(coastal_data) > 0:
                coastal_data = coastal_data[0].get('coastalRisk', {}).get('data', [])
                if coastal_data and isinstance(coastal_data, list) and len(coastal_data) > 0:
                    coastal_data = coastal_data[0]
                else:
                    coastal_data = {}
            else:
                coastal_data = {}
        except Exception as e:
            print(f"Error processing coastal data for precisely_id {precisely_id}: {str(e)}")
            coastal_data = {}

        flood_data = {}
        try:
            flood_data = result.get('flood_risk_response', {}).get('data', {}).get('getById', {}).get('addresses', {}).get('data', [{}])[0].get('floodRisk', {}).get('data', [{}])[0]
        except Exception as e:
            print(f"Error processing flood data for precisely_id {precisely_id}: {str(e)}")
            flood_data = {}

        return precisely_id, psyte_data, coastal_data, flood_data

    def is_valid_data(self, data):
        return data is not None and data != {}

    def flatten_and_prefix(self, data, prefix):
        if not self.is_valid_data(data):
            return pd.DataFrame()
        flat_data = json_normalize(data)
        return flat_data.add_prefix(f'{prefix}_')

    def process_single_result(self, result):
        precisely_id, psyte, coastal, flood = self.extract_data(result)
        
        # We'll always process psyte data if available
        psyte_flat = self.flatten_and_prefix(psyte, 'psyte')
        coastal_flat = self.flatten_and_prefix(coastal, 'coastal')
        flood_flat = self.flatten_and_prefix(flood, 'flood')
        
        combined_row = pd.concat([psyte_flat, coastal_flat, flood_flat], axis=1)
        combined_row['precisely_id'] = precisely_id
        
        return combined_row

    def create_combined_dataframe(self):
        combined_data = []
        for result in self.results:
            try:
                processed_result = self.process_single_result(result)
                if not processed_result.empty:
                    combined_data.append(processed_result)
            except Exception as e:
                print(f"Error processing result for precisely_id {result.get('precisely_id', 'unknown')}: {str(e)}")
        
        if not combined_data:
            print("No valid data to create dataframe.")
            return
        
        self.combined_df = pd.concat(combined_data, ignore_index=True)
        
        # Reorder columns
        cols = self.combined_df.columns.tolist()
        cols = ['precisely_id'] + [col for col in cols if col != 'precisely_id']
        self.combined_df = self.combined_df[cols]

    def get_dataframe(self):
        if self.combined_df is None:
            self.create_combined_dataframe()
        return self.combined_df

    def save_to_csv(self, filename='./data/combined_precisely_data.csv'):
        if self.combined_df is None:
            self.create_combined_dataframe()
        
        if self.combined_df is not None and not self.combined_df.empty:
            if os.path.exists(filename):
                # Read existing CSV file
                existing_df = pd.read_csv(filename)
                
                # Append new data to existing data
                updated_df = pd.concat([existing_df, self.combined_df], ignore_index=True)
                
                # Remove duplicates based on 'precisely_id'
                updated_df = updated_df.drop_duplicates(subset='precisely_id', keep='last')
                
                # Save the updated dataframe
                updated_df.to_csv(filename, index=False)
                print(f"Data appended to existing file {filename}")
            else:
                # If file doesn't exist, save as new file
                self.combined_df.to_csv(filename, index=False)
                print(f"Data saved to new file {filename}")
        else:
            print("No data to save.")

    def print_info(self):
        if self.combined_df is None:
            self.create_combined_dataframe()
        if self.combined_df is not None and not self.combined_df.empty:
            print(self.combined_df.info())
        else:
            print("No data available to display.")

if __name__ == "__main__":
    # client_id = input("Enter your client ID: ")
    # client_secret = input("Enter your client secret: ")
    client_id = "write your client id here"  # delete this or replace this with a dummy when sharing
    client_secret = "write your client secret here"  # delete this or replace this with a dummy when sharing
    df = pd.read_csv('./data/filtered_data.csv')[0:40] ### Rishika
    # df = pd.read_csv('./data/filtered_data.csv')[20001:40000] ### Vaishali
    # df = pd.read_csv('./data/filtered_data.csv')[40000:60000] ### Mirudula
    # df = pd.read_csv('./data/filtered_data.csv')[60000:75000] ### Manish
    # df = pd.read_csv('./data/filtered_data.csv')[75000:] ### Govardhan
    precisely_api = demographicsDataPrecisely(client_id, client_secret)
    results = precisely_api.process_dataframe(df)
    print("Processing complete. Results: ", results)
    processor = dataProcessorForDemographics(results)
    combined_df = processor.get_dataframe()
    print("Combined Dataframe:")
    print(combined_df.head())
    processor.save_to_csv()
    processor.print_info()