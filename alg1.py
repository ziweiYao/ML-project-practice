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
    X = df[features]
    y = df[target]

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

if __name__ == "__main__":
    run_linear_regression()
   