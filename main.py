import pandas as pd

# Paths to your CSV files
file1 = './Products.csv'
file2 = './Transactions.csv'

# Read the CSV files
Products = pd.read_csv(file1)
Transactions = pd.read_csv(file2)

# Display the first few rows of the DataFrames
print("Products Data:")
print(Products.head())

print("Transactions Data:")
print(Transactions.head())

# Merge tables using the Product field
enriched_data = pd.merge(Products, Transactions, on='Product')

print(enriched_data.head())

df = pd.DataFrame(enriched_data)
print(df.columns)

# Split the 'Segement_Country' column into 'Segment' and 'Country'
df[['Segment', 'Country']] = df['Segement_Country'].str.split('-', expand=True)

# Drop the original 'Segement_Country' column if it's no longer needed
df = df.drop(columns=['Segement_Country'])

# Display the updated DataFrame
print(df)

#Check if no data missing after mergin

print(df.isnull().sum())  # Check for nulls in any column
print(f"Rows in Products DataFrame: {Products.shape[0]}")
print(f"Rows in Transactions DataFrame: {Transactions.shape[0]}")
print(f"Rows in Enriched Data: {df.shape[0]}")
print(df.columns)
print(df['Product'].value_counts())