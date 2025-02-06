import pandas as pd
#---------------part 1------------------
# Paths to your CSV files
file1 = './Products.csv'
file2 = './Transactions.csv'

# Read the CSV files
Products = pd.read_csv(file1)
Transactions = pd.read_csv(file2)

#---------------part 3------------------
# Merge tables using the Product field
df_transactions = pd.DataFrame(Transactions)
df_Products = pd.DataFrame(Products)
df_transactions.columns = df_transactions.columns.str.strip()
df_Products.columns = df_Products.columns.str.strip()
enriched_data = pd.merge(df_transactions, df_Products, on="Product", how="left")

df = pd.DataFrame(enriched_data)
output_filename = "mergin_table.csv"
df.to_csv(output_filename, index=False)

# Split the 'Segement_Country' column into 'Segment' and 'Country'
df[['Segment', 'Country']] = df['Segement_Country'].str.split('-', expand=True)

# Drop the original 'Segement_Country' column if it's no longer needed
df = df.drop(columns=['Segement_Country'])
output_filename = "drop_segement_country.csv"
df.to_csv(output_filename, index=False)

#Check if no data missing after mergin
print(df.isnull().sum())  # Check for nulls in any column
print(f"Rows in Products DataFrame: {Products.shape[0]}")
print(f"Rows in Transactions DataFrame: {Transactions.shape[0]}")
print(f"Rows in Enriched Data: {df.shape[0]}")
print(df.columns)
print(df['Product'].value_counts())


#---------------part 4------------------
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

output_filename = "after_clean_price_columns.csv"
df.to_csv(output_filename, index=False)

#---------------part 5------------------
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
        print(row['Date'])
        # corrected_date = pd.Timestamp(year=2013, month=row['Month Number'], day=1)  # Default correction

    return corrected_date

# Identify unusual or missing dates
df['Date'] = df.apply(validate_and_fix_date, axis=1)

# Format the Corrected Date into a single consistent format
df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

output_filename = "after_fix_and_format_of_date.csv"
df.to_csv(output_filename, index=False)

#---------------part 6------------------
#create new Discount Impact column
df['Discount Impact'] = df['Discounts'] / df['Sales']

# Format Discount Impact as a percentage (optional)
df['Discount Impact (%)'] = df['Discount Impact'] * 100

df['Profit Margin'] = (df['Profit'] / df['Sales']) * 100
output_filename = "create_three_new_columns.csv"
df.to_csv(output_filename, index=False)

# Metric 1: Revenue Contribution
df['Revenue Contribution (%)'] = (df['Gross Sales'] / df['Gross Sales'].sum()) * 100

# Metric 2: Discount Effectiveness
# Avoid dividing by zero for entries with no discounts
df['Discount Effectiveness'] = df.apply(
    lambda row: row['Profit'] / row['Discounts'] if row['Discounts'] != 0 else None, axis=1
)

# Metric 3: Cost-to-Sales Ratio
df['Cost-to-Sales Ratio (%)'] = (df['COGS'] / df['Sales']) * 100
output_filename = "create_three_metrics.csv"
df.to_csv(output_filename, index=False)

#---------------part 7------------------
# Pivot Table for Product
product_table = pd.pivot_table(
    df,
    values=["Profit Margin", "Discount Impact", "Profit", "Sales"],
    index="Product",
    aggfunc={
        "Profit Margin": "mean",
        "Discount Impact": "mean",
        "Profit": "sum",
        "Sales": "sum"
    }
).rename(columns={
    "Profit Margin": "Avg_Profit_Margin",
    "Discount Impact": "Avg_Discount_Impact",
    "Profit": "Total_Profit",
    "Sales": "Total_Sales"
})
product_table["Total_Transactions"] = df.groupby("Product")["Units Sold"].count()
output_filename = "product_table_using_pivot.csv"
product_table.to_csv(output_filename, index=False)

# Pivot Table for Segment
segment_table = pd.pivot_table(
    df,
    values=["Profit Margin", "Discount Impact", "Profit", "Sales"],
    index="Segment",
    aggfunc={
        "Profit Margin": "mean",
        "Discount Impact": "mean",
        "Profit": "sum",
        "Sales": "sum"
    }
).rename(columns={
    "Profit Margin": "Avg_Profit_Margin",
    "Discount Impact": "Avg_Discount_Impact",
    "Profit": "Total_Profit",
    "Sales": "Total_Sales"
})
segment_table["Total_Transactions"] = df.groupby("Segment")["Units Sold"].count()
output_filename = "segment_table_using_pivot.csv"
segment_table.to_csv(output_filename, index=False)

# Pivot Table for Country
country_table = pd.pivot_table(
    df,
    values=["Profit Margin", "Discount Impact", "Profit", "Sales"],
    index="Country",
    aggfunc={
        "Profit Margin": "mean",
        "Discount Impact": "mean",
        "Profit": "sum",
        "Sales": "sum"
    }
).rename(columns={
    "Profit Margin": "Avg_Profit_Margin",
    "Discount Impact": "Avg_Discount_Impact",
    "Profit": "Total_Profit",
    "Sales": "Total_Sales"
})
country_table["Total_Transactions"] = df.groupby("Country")["Units Sold"].count()
output_filename = "country_table_using_pivot.csv"
country_table.to_csv(output_filename, index=False)

#---------------part 8----------------------
df['Year'] = pd.to_datetime(df['Date']).dt.year

# Monthly Trends Analysis
monthly_trends = df.groupby('Month Number').agg(
    Total_Sales=('Sales', 'sum'),
    Total_Profit=('Profit', 'sum'),
    Avg_Discount_Impact=('Discount Impact', 'mean'),
    Avg_Profit_Margin=('Profit Margin', 'mean')
).reset_index()

output_filename = "monthly_trends.csv"
df.to_csv(output_filename, index=False)

# Annual Trends Analysis
annual_trends = df.groupby('Year').agg(
    Total_Sales=('Sales', 'sum'),
    Total_Profit=('Profit', 'sum'),
    Avg_Discount_Impact=('Discount Impact', 'mean'),
    Avg_Profit_Margin=('Profit Margin', 'mean')
).reset_index()

output_filename = "annual_trends.csv"
df.to_csv(output_filename, index=False)

#---------------part 9-------------------
# Create a Quarter column
df['Quarter'] = pd.to_datetime(df['Date']).dt.quarter

# Group by Quarter and Product to calculate quarterly trends
quarterly_trends = df.groupby(['Quarter', 'Product']).agg(
    Total_Sales=('Sales', 'sum'),
    Avg_Profit_Margin=('Profit Margin', 'mean'),
    Total_Discounts=('Discounts', 'sum'),
    Total_Profit=('Profit', 'sum')
).reset_index()

output_filename = "quarterly_trends.csv"
quarterly_trends.to_csv(output_filename, index=False)

# Rename columns for clarity
quarterly_trends = quarterly_trends.rename(columns={
    'Total_Sales': 'Total_Quarterly_Sales',
    'Avg_Profit_Margin': 'Avg_Quarterly_Profit_Margin',
    'Total_Discounts': 'Total_Quarterly_Discounts',
    'Total_Profit': 'Total_Quarterly_Profit'
})

# Define the thresholds for underperforming products
low_profit_threshold = 0.07  # Profit Margin < 7%
high_discount_threshold = 0.085  # Discount Impact > 8.5% (Top Quartile)


output_filename = "underperforming_products_before.csv"
df.to_csv(output_filename, index=False)
# Filter the data based on criteria
underperforming_products = df[
    (df['Profit Margin'] < low_profit_threshold) &
    (df['Discount Impact'] > high_discount_threshold)
]

output_filename = "underperforming_products.csv"
underperforming_products.to_csv(output_filename, index=False)


# Select and sort the required columns
underperforming_products = underperforming_products[[
    'Product', 'Segment', 'Sales', 'Profit', 'Discount Impact', 'Profit Margin'
]].sort_values(by='Profit Margin', ascending=True)

output_filename = "underperforming_products.csv"
underperforming_products.to_csv(output_filename, index=False)

# Rename columns for better readability
underperforming_products = underperforming_products.rename(columns={
    'Sales': 'Total Sales'
})


