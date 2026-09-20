import pandas as pd

def clean_data(input_path, output_path):
    df = pd.read_csv(input_path, encoding='latin1')

    # Standardize column names
    df.columns = [c.strip().replace(' ', '_').replace('-', '_').lower() for c in df.columns]

    # Drop exact duplicate rows
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    print(f"Removed {before - after} duplicate rows")

    # Handle missing values in key columns
    df = df.dropna(subset=['sales'])

    # Add a row ID since this dataset has no order ID
    df = df.reset_index(drop=True)
    df['row_id'] = df.index + 1

    df.to_csv(output_path, index=False)
    print(f"Cleaned data saved to {output_path}, {len(df)} rows")
    return df

if __name__ == "__main__":
    clean_data("data/superstore_raw.csv", "data/superstore_clean.csv")