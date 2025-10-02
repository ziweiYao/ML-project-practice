# alg1.py
import pandas as pd

import numpy as np

import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score


# Baseline linear regression
def run_linear_regression(df):
    # Load the dataset
    df1 = df.copy()

    #change time to year_which week in the year
    # Adding more features for analysis: Average time to delivery and order value per week
    weekly_features = df.groupby('week_year').agg({
        'delivery_time': 'mean',
        'order_total_price': 'mean',
        'product_size_score': 'mean',
        'satisfaction': 'mean',
        'customer_city_freq': 'mean',
        'seller_state_freq' : 'mean'

    }).reset_index()

    from sklearn.preprocessing import MinMaxScaler
    scaler = MinMaxScaler()
    df = weekly_features.copy()
    df[['delivery_time', 'order_total_price','product_size_score', 'satisfaction', 'customer_city_freq', 'seller_state_freq']] = scaler.fit_transform(
        df[['delivery_time', 'order_total_price','product_size_score', 'satisfaction', 'customer_city_freq', 'seller_state_freq']]
    )

    # Updated feature set (all numeric)
    features = [
        'order_total_price',
        'product_size_score',
        'customer_city_freq',
        'seller_state_freq',
        'delivery_time'
    ]

    target = 'satisfaction'


    # Train/test split
    X = df1[features]
    y = df1[target]


    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=423)

    # Train the model
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Predict
    y_pred = model.predict(X_test)

    # Plot: Actual vs Predicted
    plt.figure(figsize=(8, 5))
    plt.scatter(y_test, y_pred, alpha=0.5)
    plt.plot([0, 1], [0, 1], '--r')
    '''
    plt.plot([min(y_test.min(), y_pred.min()), max(y_test.max(), y_pred.max())],
         [min(y_test.min(), y_pred.min()), max(y_test.max(), y_pred.max())],
         '--', linewidth=1)
    '''
    plt.xlabel("Actual Satisfaction")
    plt.ylabel("Predicted Satisfaction")
    plt.title("Linear Regression (v2): Satisfaction Prediction")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # Metrics
    print("R² Score:", r2_score(y_test, y_pred))
    print("Mean Squared Error:", mean_squared_error(y_test, y_pred))


# Decision Tree as alg2
from sklearn.tree import DecisionTreeRegressor
def run_decision_tree(df):
    df2 = df.copy()

    # Extract year from 'week_year'
    df2['year'] = df2['week_year'].apply(lambda x: int(str(x)[:4]))

    # Feature columns
    features = [
        'order_total_price',
        'product_size_score',
        'customer_city_freq',
        'seller_state_freq',
        'delivery_time',
        'year'
    ]

    target = 'satisfaction'

    X = df2[features]
    y = df2[target].astype(float)

    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_squared_error, r2_score
    import matplotlib.pyplot as plt

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train Decision Tree with controlled depth to reduce overfitting
    model = DecisionTreeRegressor(max_depth=7, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    # Plot predictions
    plt.figure(figsize=(8, 6))
    plt.scatter(y_test, y_pred, alpha=0.6)
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--')
    plt.xlabel("Actual Satisfaction")
    plt.ylabel("Predicted Satisfaction")
    plt.title("Decision Tree Regression (Actual vs Predicted)")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    print("Decision Tree Regression Results:")
    print("R² Score:", r2_score(y_test, y_pred))
    print("Mean Squared Error:", mean_squared_error(y_test, y_pred))



import sklearn.tree
from sklearn.ensemble import RandomForestRegressor
def run_random_forest_decisiontree(df):
    df3 = df.copy()
    target = 'satisfaction'

    features = [
        'order_total_price',
        'product_size_score',
        'customer_city_freq',
        'seller_state_freq',
        'delivery_time'
    ]

    X = df3[features]
    y = df3[target].astype(float)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Initialize the Random Forest Regressor(100 trees)
    rf_regressor = RandomForestRegressor(n_estimators=100, random_state=42)

    # Train the model
    rf_regressor.fit(X_train, y_train)

    # Make predictions on the test set
    y_pred = rf_regressor.predict(X_test)

    # Evaluate the model performance
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    # -------------------------------
    # Comparison of pred and true label
    # -------------------------------

    plt.figure(figsize=(8, 6))
    plt.scatter(y_test, y_pred, alpha=0.7, label='Data Points')
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], color='red', linestyle='--', label='Ideal Fit (y=x)')
    plt.xlabel("Actual Satisfaction")
    plt.ylabel("Predicted Satisfaction")
    plt.title("Random Forest Regression: Actual vs. Predicted Satisfaction")
    plt.legend()
    plt.show()

    print("Random Forest Regression Results:")
    print("Mean Squared Error:", mse)
    print("R² Score:", r2)

if __name__ == "__main__":
    run_linear_regression()
