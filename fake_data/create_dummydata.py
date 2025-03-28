from faker import Faker
import random
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO

fake = Faker("ko_KR")

genders = ['남성', '여성']
device_types = ["Mobile", "Desktop", "Tablet"]
trend_data = pd.read_csv("./shopping_trends.csv")
product_name = trend_data.Item_Purchased.to_list()
product_cost = trend_data.Purchase_Amount.to_list()
season = ['Spring', 'Summer', 'Fall', 'Winter']
subscribed = ['Y','N']
subscribed_weights = [0.85, 0.15]
paths = [
        "Viewed Ad",
        "Visited Blog",
        "Browsed App",
        "Clicked App Banner",
        "Viewed Product Page",
        "Checked Customer Reviews",
        "Looked at Special Offers",
        "Checked Order Status",
        "Contacted Support",
        "Shared on Social Media"
    ]

def get_season():
    current_month = datetime.now().month
    if 3 <= current_month <= 5:
        return "Spring"
    elif 6 <= current_month <= 8:
        return "Summer"
    elif 9 <= current_month <= 11:
        return "Autumn"
    else:
        return "Winter"

season = get_season()

def generate_purchase_history(num_records):
    purchase_history = []
    purchase_user = []
    customer_behavior = []
    for _ in range(num_records):
        # 거래데이터

        purchased_data = {
            'transaction_id': fake.uuid4(),
            'purchase_date': fake.date_time_this_year(),
            'product_name': random.sample(product_name, 1)[0],
            'quantity': random.randint(1, 5),
            'cost': random.sample(product_cost, 1)[0],
            'product_season': random.sample(season, 1),
            'review_rating': random.randint(1, 5),
            'promo_code_used' : random.sample(subscribed, 1),
            'stayed_time': random.randint(30, 300),
            'visit_path' : random.sample(paths, 1),
            'device_type': random.choice(device_types)
        },
        customer_data = {
            'customer_id': fake.uuid4(),
            'sex': random.choice(genders), 
            'name': fake.name(),  
            'age': random.randint(18, 65),
            'job': fake.job(),
            'address' : fake.address(),
            'joined_date' : fake.date_between(start_date="-5y"),
            'subscribe_type' : random.sample(subscribed, 1)[0],
            'pay_method_enrolled' : random.sample(subscribed, 1)[0]
        },
        customer_data = {
            'is_returned': random.choices(subscribed, weights=subscribed_weights, k=1)[0],
            'visit_count': random.randint(1,10),
            #minuite
            'cart_time': random.randint(1, 600),
            'purchase_hour': datetime.now().strftime("%H"),
            'season': season
        }

        purchase_history.append(purchased_data)
        purchase_user.append(customer_data)
        customer_behavior.append(customer_data)

    return pd.DataFrame(purchase_history), pd.DataFrame(purchase_user), pd.DataFrame(customer_behavior)

def save_to_s3(df, bucket_name, file_prefix, df_name=None):
    s3_client = boto3.client('s3')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    date_str = datetime.now().strftime("%Y-%m-%d")
    if df_name == 'purchase_history':
        file_name = f"chaos_shop/purchase_history/{date_str}/{file_prefix}.csv"
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
    elif df_name == 'purchase_user':
        file_name = f"chaos_shop/purchase_user/{date_str}/{file_prefix}.csv"
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
    else:
        file_name = f"chaos_shop/customer_behavior/{date_str}/{file_prefix}.csv"
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())

    print(f"{file_name} uploaded to {bucket_name}")

purchase_history, purchase_user, customer_behavior = generate_purchase_history(100000)

#change bucket name
bucket_name = "your_bucket_name"
save_to_s3(purchase_history, bucket_name, fake.uuid4(), df_name='purchase_history')
save_to_s3(purchase_user, bucket_name, fake.uuid4(), df_name='purchase_user')
save_to_s3(customer_behavior, bucket_name, fake.uuid4(), df_name='customer_behavior')