import pandas as pd
import argparse

class DataCleaner:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = pd.read_csv(self.file_path, low_memory=False)
    
    def clean_data(self):
        self.drop_na()
        self.handle_missing_values()
        self.convert_numeric_columns()
        self.convert_date_columns()
        self.remove_duplicates()
        self.standardize_string_formatting()
    
    def drop_na(self):
        """Drop rows with missing PBKEY."""
        self.df = self.df.dropna(subset=['PBKEY'])
    
    def handle_missing_values(self):
        """Fill missing values in numeric and categorical columns."""
        numeric_cols = self.df.select_dtypes(include=['float64', 'int64']).columns
        categorical_cols = self.df.select_dtypes(include=['object']).columns
        
        self.df.loc[:, numeric_cols] = self.df[numeric_cols].fillna(self.df[numeric_cols].median())
        self.df.loc[:, categorical_cols] = self.df[categorical_cols].fillna(self.df[categorical_cols].mode().iloc[0])
    
    def convert_numeric_columns(self):
        """Convert specified columns to numeric data types."""
        cols_to_convert = ['PLUS4', 'GEOID', 'LAT', 'LON', 'flood_communityNumber', 
                           'flood_addressLocationElevationFeet', 'flood_year100FloodZoneDistanceFeet', 
                           'flood_year500FloodZoneDistanceFeet', 'flood_distanceToNearestWaterbodyFeet']
        self.df.loc[:, cols_to_convert] = self.df[cols_to_convert].apply(pd.to_numeric, errors='coerce')
    
    def convert_date_columns(self):
        """Convert specified columns to datetime format."""
        date_cols = ['flood_mapEffectiveDate', 'flood_floodHazardBoundaryMapInitialDate', 
                     'flood_floodInsuranceRateMapInitialDate']
        self.df.loc[:, date_cols] = self.df[date_cols].apply(pd.to_datetime, errors='coerce', format='%d-%m-%Y')
    
    def remove_duplicates(self):
        """Remove duplicate rows."""
        self.df = self.df.drop_duplicates()
    
    def standardize_string_formatting(self):
        """Standardize string formatting in categorical columns."""
        categorical_cols = self.df.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            self.df.loc[:, col] = self.df[col].astype(str).str.strip()
    
    def save_cleaned_data(self, output_path='../data/cleanedData.csv'):
        """Save the cleaned DataFrame to a CSV file."""
        self.df.to_csv(output_path, index=False)
        print(f"Cleaned data saved to {output_path}")

def main():
    # Command line argument parsing
    parser = argparse.ArgumentParser(description="Clean a dataset.")
    parser.add_argument('dataframePath', type=str, help='Path to the CSV file to clean')
    args = parser.parse_args()

    # Instantiate the DataCleaner with the provided path
    cleaner = DataCleaner(args.dataframePath)
    
    # Perform the cleaning process
    cleaner.clean_data()
    
    # Save the cleaned data
    cleaner.save_cleaned_data()

if __name__ == '__main__':
    main()
