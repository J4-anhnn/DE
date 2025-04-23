import pandas as pd
import os
from src.config import RAW_DATA_PATH, OUTPUT_DATA_PATH

def run_etl():
    df = pd.read_csv(RAW_DATA_PATH)
    filtered_df = df[df['Amount'] > 60]
    total = filtered_df['Amount'].sum()
    output_df = pd.DataFrame({'TotalAmountOver100': [total]})

    os.makedirs(os.path.dirname(OUTPUT_DATA_PATH), exist_ok=True)

    output_df.to_csv(OUTPUT_DATA_PATH, index=False)
    print("ETL completed. Output saved to:", OUTPUT_DATA_PATH)

if __name__ == "__main__":
    run_etl()
