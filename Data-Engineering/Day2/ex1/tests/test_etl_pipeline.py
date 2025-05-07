
import pandas as pd
from src.etl_pipeline import run_etl

def test_run_etl(tmp_path):
    # Create sample input file
    sample_data = pd.DataFrame({
        'Category': ['A', 'B', 'C'],
        'Amount': [50, 150, 200]
    })
    input_file = tmp_path / "sales.csv"
    sample_data.to_csv(input_file, index=False)

    # Override paths
    import src.config as config
    config.RAW_DATA_PATH = str(input_file)
    config.OUTPUT_DATA_PATH = str(tmp_path / "processed_sales.csv")

    # Run ETL
    run_etl()

    # Check output
    output_df = pd.read_csv(config.OUTPUT_DATA_PATH)
    assert output_df['TotalAmount'][0] == 350
