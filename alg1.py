# alg1.py
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

# Baseline linear regression
def run_linear_regression():
    # Load the dataset
    df = pd.read_csv("data.csv")

    # Updated feature set (all numeric)
    features = [
        'price',
        'freight_value',
        'product_size_score',
        'late_delivery',
        'time_of_delay',
        'customer_city_freq',
        'seller_state_freq'
    ]
    target = 'satisfaction'

    # Drop rows with missing values
    df = df[features + [target]].dropna()

    # Train/test split
    X = df[features]
    y = df[target]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train the model
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Predict
    y_pred = model.predict(X_test)

    # Plot: Actual vs Predicted
    plt.figure(figsize=(8, 5))
    plt.scatter(y_test, y_pred, alpha=0.5)
    plt.plot([1, 5], [1, 5], '--r')
    plt.xlabel("Actual Satisfaction")
    plt.ylabel("Predicted Satisfaction")
    plt.title("Linear Regression (v2): Satisfaction Prediction")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("satisfaction_regression_plot_v2.png")
    plt.show()

    # Metrics
    print("R² Score:", r2_score(y_test, y_pred))
    print("Mean Squared Error:", mean_squared_error(y_test, y_pred))

if __name__ == "__main__":
    run_linear_regression()
