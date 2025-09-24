import pandas as pd

def createMerge_df():
    #first, read all data set as dataframes.
    customers_df = pd.read_csv('./Data/olist_customers_dataset.csv')
    order_items_df = pd.read_csv('./Data/olist_order_items_dataset.csv')
    order_payments_df = pd.read_csv('./Data/olist_order_payments_dataset.csv')
    order_reviews_df = pd.read_csv('./Data/olist_order_reviews_dataset.csv')
    orders_df = pd.read_csv('./Data/olist_orders_dataset.csv')
    sellers_df = pd.read_csv('./Data/olist_sellers_dataset.csv')
    products_df = pd.read_csv('./Data/olist_products_dataset.csv')
    product_category_name_translation_df = pd.read_csv('./Data/product_category_name_translation.csv')

        
    df = pd.merge(orders_df,customers_df , on='customer_id', how='left')
    #determine the key for join
    merge_keys = {
            'order_payments': 'order_id',
            'order_items': 'order_id',
            'order_reviews': 'order_id',
            'sellers': 'seller_id',
            'products': 'product_id',
    }
    #name the df keys
    allDatas = {
        'customers_dataset': customers_df,
        'order_items': order_items_df,
        'order_payments': order_payments_df,
        'order_reviews': order_reviews_df,
        'orders_dataset': orders_df,
        'sellers': sellers_df,
        'products': products_df,
        'product_category_name_translation': product_category_name_translation_df
    }
    for name, key in merge_keys.items():
        if name in allDatas:
            df = pd.merge(df, allDatas[name], on=key, how='left')
    return df


def dropNull(df):
    required_non_null_percentage = int(input("enter the minimum percentage of null value you want ___%   \ntype between 100 to 0:"))
    required_non_null_percentage = required_non_null_percentage / 100
    if 0 <= required_non_null_percentage <= 100 :
        # Calculate the threshold (minimum number of non-null values)
        threshold_value = int(len(df.columns) * required_non_null_percentage)
        df = df.dropna(thresh=threshold_value, axis=0)
    else:
        threshold_value = int(len(df.columns) * 1)
        df = df.dropna(thresh=threshold_value, axis=0)
    return df


def date_time_convert(df,datetime_cols):
    df = df.copy()
    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    return df


def preprocessing(df):
    #eliminate null values
    #df.info()
    df = dropNull(df)
    datetime_cols = ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 
                    'order_delivered_customer_date', 'order_estimated_delivery_date', 
                    'shipping_limit_date', 'review_creation_date', 'review_answer_timestamp']
    df = date_time_convert(df,datetime_cols)
    
    df['delivery_time'] = (df['order_delivered_customer_date'] - df['order_approved_at']).dt.days
    df['order_processing_time'] = (df['order_approved_at'] - df['order_purchase_timestamp']).dt.days
    df['estimated_minus_actual_shipping'] = (df['order_estimated_delivery_date'] - df['order_delivered_customer_date']).dt.days
    df['product_volume'] = (df['product_length_cm'] * df['product_width_cm'] * df['product_height_cm'])
    df['product_size_score'] = df['product_volume'] / df['product_volume'].max()
    df['satisfaction'] = (df['review_score'] >= df['review_score'].mean()).astype(int)
    df['order_total_price'] = df['price'] + df['freight_value']
    df['late_delivery'] = (df['order_delivered_customer_date'] > df['order_estimated_delivery_date']).astype(int)
    
    obsolete_cols = ['order_delivered_customer_date', 'order_approved_at', 'order_approved_at',
                     'order_estimated_delivery_date',  'product_length_cm', 'product_width_cm', 
                     'product_height_cm', 'product_volume', 'review_score' ]
    
    df.drop(columns = obsolete_cols, inplace=True)
    #df.info()
    
    return df


def generateHeatMap(df):
    return 0
 


#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#main, the target is satisfaction
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#create dataframe for the ml
df = createMerge_df()
df = preprocessing(df)
#generate Heat map to see the relation between attributes
generateHeatMap(df)
 
#write the data into local for checking or future use. 
df.to_csv('data.csv', index=False)
