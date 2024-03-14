from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

def has_duplicates(data: pd.DataFrame):
    feature_cols = data.columns.drop('flat_id')
    is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
    if  len(data[is_duplicated_features]) > 0:
        return True
    else:
        return False


def missing_values(data: pd.DataFrame):
    cols_with_nans = data.isnull().sum()
    if cols_with_nans.any():
        cols_with_nans = cols_with_nans[cols_with_nans > 0] # список имен столбцов с пропусками
        return cols_with_nans.index.drop('target') if 'target' in cols_with_nans.index else cols_with_nans.index
    else:
        return []

def fill_missing_values(cols_with_nans: pd.Series, data: pd.DataFrame):
    for col in cols_with_nans:
        if data[col].dtype in [float, int]:
            fill_value = data[col].mean()
        elif data[col].dtype == 'object':
            fill_value = data[col].mode().iloc[0]

        data[col] = data[col].fillna(fill_value)
    return data

def remove_outliers(data: pd.DataFrame):
    num_cols = data.select_dtypes(['float']).columns
    threshold = 1.5
    potential_outliers = pd.DataFrame()

    for col in num_cols:
        Q1 = data[col].quantile(0.25)
        Q3 = data[col].quantile(0.75)
        IQR = Q3 - Q1
        margin = threshold * IQR
        lower = Q1 - margin
        upper = Q3 + margin
        potential_outliers[col] = ~data[col].between(lower, upper)

    outliers = potential_outliers.any(axis=1)
    data = data[~outliers]
    return data

def remove_duplicates(data: pd.DataFrame):
    feature_cols = data.columns.drop('flat_id').tolist()
    is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
    data = data[~is_duplicated_features].reset_index(drop=True)
    return data


def extract(**kwargs):
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        table_name = kwargs.get('table_name')
        sql = 'SELECT * FROM ' + table_name
        data = pd.read_sql(sql, conn)
        conn.close()
        ti = kwargs['ti']
        ti.xcom_push(key='extracted_data', value=data)

def transform(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='extract', key='extracted_data')

        if has_duplicates(data):
            data = remove_duplicates(data)

        cols_with_nans = missing_values(data)
        if cols_with_nans:
            data = fill_missing_values(cols_with_nans, data)
        
        data = remove_outliers(data)
        
        ti.xcom_push('transformed_data', data)