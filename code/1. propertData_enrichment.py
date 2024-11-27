import time
import base64
import requests
import threading
import pandas as pd
from tqdm import tqdm
import argparse
import pandas as pd

class propertyDataPrecisely:
    def __init__(self, client_id, client_secret, sample_percentage=100):
        self.client_id = client_id
        self.client_secret = client_secret
        self.auth_token = self.get_new_token() 
        self.token_expiry_time = time.time() + (59 * 60)  
        self.url = "https://api.cloud.precisely.com/data-graph/graphql/"
        self.sample_percentage = sample_percentage
        self.auto_refresh_token()
        self.last_processed_index = 0
        self.resume_file = "resume_data.json"

    def get_new_token(self):
        """
        Retrieve a new authentication token from Precisely API.
        """
        url = "https://api.cloud.precisely.com/auth/v2/token"
        payload = 'grant_type=client_credentials&scope=default'
        auth_string = f"{self.client_id}:{self.client_secret}"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {encoded_auth}'
        }

        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == 200:
            token_data = response.json()
            print("New auth token retrieved.")
            return token_data['access_token']
        else:
            raise Exception(f"Failed to retrieve token: {response.status_code}, {response.text}")

    def refresh_token(self):
        """
        Refresh the authentication token after 59 minutes and pause for 10 seconds.
        """
        print("Refreshing authentication token...")
        self.auth_token = self.get_new_token()
        self.token_expiry_time = time.time() + (59 * 60)
        print("New token has been released.")

    def auto_refresh_token(self):
        """
        Auto-refresh the token every 59 minutes in the background.
        """
        def refresh_loop():
            while True:
                time_remaining = self.token_expiry_time - time.time()
                if time_remaining < 60:  
                    time.sleep(10)
                    self.refresh_token() 
                time.sleep(60)

        threading.Thread(target=refresh_loop, daemon=True).start()

    def fetch_data(self, query):
        """
        Function to fetch data from API with error handling and retries.
        """
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.auth_token}"
        }
        for attempt in range(3): 
            response = requests.post(self.url, json={"query": query}, headers=headers)
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError as e:
                    print(f"Error parsing JSON: {e}")
            elif response.status_code == 401:
                print("Authentication expired. Refreshing token.")
                self.refresh_token() 
            else:
                print(f"Error: {response.status_code}, {response.text}")
            time.sleep(2)
        return None

    @staticmethod
    def safe_get(data, *keys):
        """
        Safely retrieve nested keys or return None.
        """
        for key in keys:
            if data is None:
                return None
            if isinstance(data, (dict, list)) and key in data:
                data = data[key]
            elif isinstance(data, list) and isinstance(key, int) and key < len(data):
                data = data[key]
            else:
                return None
        return data

    def get_data(self, query, *path):
        """
        Generic function to retrieve data using a GraphQL query.
        """
        response = self.fetch_data(query)
        return self.safe_get(response, *path)

    def build_address(self, row):
        """
        Format the address consistently from the DataFrame row.
        """
        return f"{row['ADD_NUMBER']} {row['STREETNAME']}, {row['CITY']}, {row['STATE']} {row['ZIPCODE']}"

    def enhance_data(self, df):
        """
        Enhance the DataFrame by fetching additional data from the Precisely API.
        """
        new_columns = [
            "LivingSquareFootage", "BedroomCount", "BathroomCount", "SaleAmount",
            "ParcelID", "ParcelArea", "Elevation", "Geometry", 
            "BuildingID", "MaxElevation", "MinElevation", "BuildingArea"
        ]
        for col in new_columns:
            df[col] = None  

        for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Enhancing Data"):
            address = self.build_address(row)

            property_data = self.get_property_data(address)
            if property_data:
                df.loc[index, "LivingSquareFootage"] = property_data.get("livingSquareFootage")
                df.loc[index, "BedroomCount"] = property_data.get("bedroomCount")
                df.loc[index, "BathroomCount"] = self.safe_get(property_data, "bathroomCount", "value")
                df.loc[index, "SaleAmount"] = property_data.get("saleAmount")

            parcel_data = self.get_parcel_data(address)
            if parcel_data:
                df.loc[index, "ParcelID"] = parcel_data.get("parcelID")
                df.loc[index, "ParcelArea"] = parcel_data.get("parcelArea")
                df.loc[index, "Elevation"] = parcel_data.get("elevation")
                df.loc[index, "Geometry"] = parcel_data.get("geometry")

            building_data = self.get_building_data(address)
            if building_data:
                df.loc[index, "BuildingID"] = building_data.get("buildingID")
                df.loc[index, "MaxElevation"] = building_data.get("maximumElevation")
                df.loc[index, "MinElevation"] = building_data.get("minimumElevation")
                df.loc[index, "BuildingArea"] = building_data.get("buildingArea")

        return df

    def get_property_data(self, address):
        query = f"""
        query {{
            getByAddress(address: "{address}") {{
                propertyAttributes {{
                    data {{
                        livingSquareFootage
                        bedroomCount
                        bathroomCount {{
                            value
                        }}
                        saleAmount
                    }}
                }}
            }}
        }}
        """
        return self.get_data(query, "data", "getByAddress", "propertyAttributes", "data", 0)

    def get_parcel_data(self, address):
        query = f"""
        query {{
            getByAddress(address: "{address}") {{
                parcels {{
                    data {{
                        parcelID
                        parcelArea
                        elevation
                        geometry
                    }}
                }}
            }}
        }}
        """
        return self.get_data(query, "data", "getByAddress", "parcels", "data", 0)

    def get_building_data(self, address):
        query = f"""
        query {{
            getByAddress(address: "{address}") {{
                buildings {{
                    data {{
                        buildingID
                        maximumElevation
                        minimumElevation
                        buildingArea
                    }}
                }}
            }}
        }}
        """
        return self.get_data(query, "data", "getByAddress", "buildings", "data", 0)

    def sample_data(self, df):
        """
        Sample a percentage of the DataFrame.
        """
        sample_size = int(len(df) * (self.sample_percentage / 100)) 
        return df.sample(n=sample_size, random_state=42)

def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Enhance property data using Precisely API.")
    parser.add_argument('--client_id', type=str, required=True, help="Client ID for the API.")
    parser.add_argument('--client_secret', type=str, required=True, help="Client secret for the API.")
    parser.add_argument('--file_path', type=str, required=True, help="Path to the input CSV file.")
    parser.add_argument('--output_path', type=str, required=True, help="Path to save the enriched CSV file.")
    parser.add_argument('--start_row', type=int, default=0, help="Starting row index for the subset of data.")
    parser.add_argument('--end_row', type=int, help="Ending row index for the subset of data.")
    parser.add_argument('--sample_percentage', type=int, default=100, help="Percentage of data to sample.")

    args = parser.parse_args()

    # Load and subset the data
    df = pd.read_csv(args.file_path)
    df = df[args.start_row:args.end_row] if args.end_row else df[args.start_row:]
    print(f"Loaded data from {args.file_path}. Subsetting rows {args.start_row} to {args.end_row}.")
    # Initialize the API and enhance the data
    api = propertyDataPrecisely(args.client_id, args.client_secret, sample_percentage=args.sample_percentage)
    sampled_df = api.sample_data(df)
    enhanced_df = api.enhance_data(sampled_df)

    # Save the enhanced data
    enhanced_df.to_csv(args.output_path, index=False)
    print(f"Data enrichment completed and saved to {args.output_path}.")

if __name__ == "__main__":
    main()
