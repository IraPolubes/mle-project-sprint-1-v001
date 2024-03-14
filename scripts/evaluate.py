import pandas as pd
import sklearn.metrics
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error #importing all, in case params metrics changes
import yaml
import joblib
import json
import os

def evaluate_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    metric_name = params['metrics']
    target_col = params['target_col']


    X_test = pd.read_csv('data/X_test.csv')
    y_test = pd.read_csv('data/y_test.csv')[target_col[0]]


    with open('models/fitted_model.pkl', 'rb') as fd:
        model = joblib.load(fd)


    y_pred = model.predict(X_test)

    metric_function = getattr(sklearn.metrics, metric_name, None)

    if metric_function is None:
        raise ValueError(f"The metric '{metric_name}' is not recognized.")

    score = metric_function(y_test, y_pred)

    os.makedirs('evaluation_results', exist_ok=True)
    with open('evaluation_results/res.json', 'w') as fd:
        json.dump({'score': score.item() if hasattr(score, 'item') else score}, fd)

if __name__ == '__main__':
    evaluate_model()