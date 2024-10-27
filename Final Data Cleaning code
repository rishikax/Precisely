import pandas as pd

# Load the dataset with 'low_memory=False' to avoid dtype warning
file_path = 'merged_data_vaishali.csv'
df = pd.read_csv(file_path, low_memory=False)

# Cleaning steps
df_cleaned = df.dropna(subset=['PBKEY'])

# Handling missing values with explicit loc usage to avoid SettingWithCopyWarning
numeric_cols = df_cleaned.select_dtypes(include=['float64', 'int64']).columns
categorical_cols = df_cleaned.select_dtypes(include=['object']).columns

df_cleaned.loc[:, numeric_cols] = df_cleaned[numeric_cols].fillna(df_cleaned[numeric_cols].median())
df_cleaned.loc[:, categorical_cols] = df_cleaned[categorical_cols].fillna(df_cleaned[categorical_cols].mode().iloc[0])

# Converting possible numeric columns to float with explicit loc usage
cols_to_convert = ['PLUS4', 'GEOID', 'LAT', 'LON', 'flood_communityNumber', 
                   'flood_addressLocationElevationFeet', 'flood_year100FloodZoneDistanceFeet', 
                   'flood_year500FloodZoneDistanceFeet', 'flood_distanceToNearestWaterbodyFeet']
df_cleaned.loc[:, cols_to_convert] = df_cleaned[cols_to_convert].apply(pd.to_numeric, errors='coerce')

# Converting date columns to datetime format
date_cols = ['flood_mapEffectiveDate', 'flood_floodHazardBoundaryMapInitialDate', 
             'flood_floodInsuranceRateMapInitialDate']
df_cleaned.loc[:, date_cols] = df_cleaned[date_cols].apply(pd.to_datetime, errors='coerce', format='%d-%m-%Y')

# Removing duplicates
df_cleaned = df_cleaned.drop_duplicates()

# Standardizing string formatting with explicit loc usage
for col in categorical_cols:
    df_cleaned.loc[:, col] = df_cleaned[col].astype(str).str.strip()

# Save the cleaned data
output_path = 'final_cleaned_merged_data_vaishali_v2.csv'
df_cleaned.to_csv(output_path, index=False)
