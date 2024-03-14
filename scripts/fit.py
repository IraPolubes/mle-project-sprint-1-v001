import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from category_encoders import CatBoostEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import numpy as np

import yaml
import os
import joblib


def fit_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    target_col = params['target_col']
    drop_index_col = [params['index_col']] + params['add_drop_index_col']
    bool_index = params['bool_index']
    cat_index = params['cat_index']

    data = pd.read_csv('data/initial_data.csv')

    X = data.drop(drop_index_col + target_col, axis=1)
    y = data[target_col[0]]

    num_features = X.drop(columns=cat_index + bool_index).columns.tolist()

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    preprocessor = ColumnTransformer(
        [
            ('cat', CatBoostEncoder(return_df=False), cat_index),
            ('num', StandardScaler(), num_features)
        ],
        remainder='drop',
        verbose_feature_names_out=False
    )

    model = LinearRegression()

    pipeline = Pipeline(
        [
            ('preprocessor', preprocessor),
            ('model', model)
        ]
    )

    pipeline.fit(X_train, y_train)

    os.makedirs('models', exist_ok=True)

    with open('models/fitted_model.pkl', 'wb') as fd:
        joblib.dump(pipeline, fd)

    X_test.to_csv('data/X_test.csv', index=False)
    y_test.to_csv('data/y_test.csv', index=False)

if __name__ == '__main__':
    fit_model()