import pandas as pd

# Paths to your CSV files
file1 = './Products.csv'
file2 = './Transactions.csv'

# Read the CSV files
Products = pd.read_csv(file1)
Transactions = pd.read_csv(file2)

# Display the first few rows of the DataFrames
# print("Products Data:")
# print(Products.head())

# print("Transactions Data:")
# print(Transactions.head())

# Merge tables using the Product field
df_transactions = pd.DataFrame(Transactions)
df_Products = pd.DataFrame(Products)
df_transactions.columns = df_transactions.columns.str.strip()
df_Products.columns = df_Products.columns.str.strip()
enriched_data = pd.merge(df_transactions, df_Products, on="Product", how="left")
# enriched_data = pd.merge(Products, Transactions, on='Product')

# print(enriched_data.head())

df = pd.DataFrame(enriched_data)
# print(df.columns)

# Split the 'Segement_Country' column into 'Segment' and 'Country'
df[['Segment', 'Country']] = df['Segement_Country'].str.split('-', expand=True)

# Drop the original 'Segement_Country' column if it's no longer needed
df = df.drop(columns=['Segement_Country'])

# Display the updated DataFrame
# print(df)

#Check if no data missing after mergin

# print(df.isnull().sum())  # Check for nulls in any column
# print(f"Rows in Products DataFrame: {Products.shape[0]}")
# print(f"Rows in Transactions DataFrame: {Transactions.shape[0]}")
# print(f"Rows in Enriched Data: {df.shape[0]}")
# print(df.columns)
# print(df['Product'].value_counts())

# Define a function to clean the financial fields
def clean_financial_field(value):
    if isinstance(value, (int, float)):  # If already a number, return it as is
        return value
    # Remove non-numeric characters (e.g., $, commas, extra dots)
    clean_value = (
        value.replace("$", "")
             .replace(",", "")
             .replace(".", "", value.count(".") - 1)  # Handle decimal points correctly
    )
    try:
        return float(clean_value)  # Convert to a float for analysis
    except ValueError:
        return None  # Return None for non-convertible values
    
# Identify the financial fields that need cleanup
financial_fields = ["Sale Price", "Gross Sales", "Discounts", "Sales", "COGS", "Profit","Manufacturing Price"]

# Apply the cleaning function to each field
for field in financial_fields:
    df[field] = df[field].apply(clean_financial_field)

# Display the cleaned DataFrame
# print(df)

# Format Date Column
def validate_and_fix_date(row):
    try:
        # Parse the existing date
        date = pd.to_datetime(row['Date'], format='%m/%d/%Y', errors='coerce')
        
        # Check if the date's month matches the Month Number column
        if date and date.month != row['Month Number']:
            print(date.month , row['Month Number'])
            # Correct the date by aligning it with the Month Number column
            corrected_date = pd.Timestamp(year=date.year, month=row['Month Number'], day=1)
        else:
            corrected_date = date
    except:
        # Handle invalid or unusual dates
        corrected_date = pd.Timestamp(year=2013, month=row['Month Number'], day=1)  # Default correction

    return corrected_date

# Identify unusual or missing dates
df['Date'] = df.apply(validate_and_fix_date, axis=1)

# Format the Corrected Date into a single consistent format
df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

#create new Discount Impact column
df['Discount Impact'] = df['Discounts'] / df['Sales']

# Format Discount Impact as a percentage (optional)
df['Discount Impact (%)'] = df['Discount Impact'] * 100

df['Profit Margin'] = (df['Profit'] / df['Sales']) * 100

print(df['Profit Margin'])