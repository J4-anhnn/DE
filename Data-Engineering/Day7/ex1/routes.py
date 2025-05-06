from flask import Blueprint, request, jsonify
import pandas as pd
import uuid
import os
from datetime import datetime, timedelta

from bigquery_utils import get_bigquery_client, load_dataframe_to_bq

bp = Blueprint('api', __name__)

@bp.route('/upload', methods=['POST'])
def upload_csv_to_bigquery():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    file = request.files['file']
    if file.filename == '' or not file.filename.endswith('.csv'):
        return jsonify({'error': 'Invalid file. Please upload a CSV file'}), 400

    dataset_id = request.form.get('dataset_id', 'bigquery_train')
    table_id = 'sales_data'
    temp_file_path = f"temp_{uuid.uuid4().hex}.csv"

    try:
        file.save(temp_file_path)
        try:
            df = pd.read_csv(
                temp_file_path,
                parse_dates=['Date', 'Target Close'],
                date_parser=lambda x: pd.to_datetime(x, format='%m/%d/%Y'),
                dtype={
                    'Closed Opportunity': str,
                    'Active Opportunity': str,
                    'Latest Status Entry': str
                }
            )

            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            df['Target Close'] = pd.to_datetime(df['Target Close']).dt.strftime('%Y-%m-%d')
            df['Closed Opportunity'] = df['Closed Opportunity'].apply(lambda x: str(x).upper() == 'TRUE')
            df['Active Opportunity'] = df['Active Opportunity'].apply(lambda x: str(x).upper() == 'TRUE')
            df['Latest Status Entry'] = df['Latest Status Entry'].apply(
                lambda x: datetime.now().strftime('%Y-%m-%d %H:%M:%S') if str(x).upper() == 'TRUE' else None
            )
            if df.empty:
                raise ValueError("CSV file is empty")
        except Exception as e:
            return jsonify({'error': f'Error reading CSV file: {str(e)}'}), 400

        client = get_bigquery_client()
        table = load_dataframe_to_bq(df, client, dataset_id, table_id)

        os.remove(temp_file_path)

        return jsonify({
            'success': True,
            'message': f'Data uploaded successfully to {dataset_id}.{table_id}',
            'rows': table.num_rows,
            'columns': len(table.schema),
            'schema': [{'name': field.name, 'type': field.field_type} for field in table.schema]
        })
    except Exception as e:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        return jsonify({'error': str(e)}), 500
    
# API: Total forecasted revenue by Salesperson
@bp.route('/revenue-by-salesperson', methods=['GET'])
def revenue_by_salesperson():
    dataset_id = request.args.get('dataset_id', 'bigquery_train')
    table_id = 'sales_data'
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    client = get_bigquery_client()
    table_full = f'{client.project}.{dataset_id}.{table_id}'

    # Build partition filter
    partition_filter = ""
    if start_date and end_date:
        partition_filter = f"WHERE Date BETWEEN '{start_date}' AND '{end_date}'"
    elif start_date:
        partition_filter = f"WHERE Date >= '{start_date}'"
    elif end_date:
        partition_filter = f"WHERE Date <= '{end_date}'"

    query = f"""
        SELECT
          Salesperson AS sales_person,
          COUNT(`Lead Name`) AS num_leads,
          SUM(`Forecasted Monthly Revenue`) AS total_revenue
        FROM `{table_full}`
        {partition_filter}
        GROUP BY Salesperson
        ORDER BY total_revenue DESC
    """
    query_job = client.query(query)
    results = [dict(row) for row in query_job]

    return jsonify(results)

# API: Total lead count by region
@bp.route('/lead-count-by-region', methods=['GET'])
def lead_count_by_region():
    dataset_id = request.args.get('dataset_id', 'bigquery_train')
    table_id = 'sales_data'
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    client = get_bigquery_client()
    table_full = f'{client.project}.{dataset_id}.{table_id}'

    # Build partition filter
    partition_filter = ""
    if start_date and end_date:
        partition_filter = f"WHERE Date BETWEEN '{start_date}' AND '{end_date}'"
    elif start_date:
        partition_filter = f"WHERE Date >= '{start_date}'"
    elif end_date:
        partition_filter = f"WHERE Date <= '{end_date}'"

    query = f"""
        SELECT
          Region,
          COUNT(*) AS num_leads,
          SUM(`Forecasted Monthly Revenue`) AS total_revenue
        FROM `{table_full}`
        {partition_filter}
        GROUP BY Region
        ORDER BY total_revenue DESC
    """
    query_job = client.query(query)
    results = [dict(row) for row in query_job]

    return jsonify(results)

# API: Top 3 Leads by forecasted revenue
@bp.route('/top-leads', methods=['GET'])
def top_leads():
    dataset_id = request.args.get('dataset_id', 'bigquery_train')
    table_id = 'sales_data'
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    client = get_bigquery_client()
    table_full = f'{client.project}.{dataset_id}.{table_id}'

    # Build partition filter
    partition_filter = ""
    if start_date and end_date:
        partition_filter = f"WHERE Date BETWEEN '{start_date}' AND '{end_date}'"
    elif start_date:
        partition_filter = f"WHERE Date >= '{start_date}'"
    elif end_date:
        partition_filter = f"WHERE Date <= '{end_date}'"

    query = f"""
        SELECT
          `Lead Name`,
          Salesperson,
          `Forecasted Monthly Revenue`
        FROM `{table_full}`
        {partition_filter}
        ORDER BY `Forecasted Monthly Revenue` DESC
        LIMIT 3
    """
    query_job = client.query(query)
    results = [dict(row) for row in query_job]

    return jsonify(results)

# API: Monthly revenue trends with partition filtering
@bp.route('/monthly-revenue', methods=['GET'])
def monthly_revenue():
    """Get monthly revenue trends with partition filtering."""
    dataset_id = request.args.get('dataset_id', 'bigquery_train')
    table_id = 'sales_data'
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    client = get_bigquery_client()
    table_full = f'{client.project}.{dataset_id}.{table_id}'

    # Build partition filter
    partition_filter = ""
    if start_date and end_date:
        partition_filter = f"WHERE Date BETWEEN '{start_date}' AND '{end_date}'"
    elif start_date:
        partition_filter = f"WHERE Date >= '{start_date}'"
    elif end_date:
        partition_filter = f"WHERE Date <= '{end_date}'"

    query = f"""
        SELECT
          DATE_TRUNC(Date, MONTH) as month,
          SUM(`Forecasted Monthly Revenue`) as total_revenue,
          COUNT(*) as num_opportunities
        FROM `{table_full}`
        {partition_filter}
        GROUP BY month
        ORDER BY month DESC
    """
    query_job = client.query(query)
    results = [dict(row) for row in query_job]

    return jsonify(results)