import json
from random import randrange
from datetime import timedelta, datetime
import string
import random
import uuid



#1 Generate a file with 1000 or more rows using the df_data_generator.py program
out_path = r"output1.txt"
with open(out_path, "a") as file:
    file.write("title,visitId,name,persistentCart,timestamp" + "\n")

def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

d1 = datetime.strptime('1/21/2025 12:00 AM', '%m/%d/%Y %I:%M %p')
d2 = datetime.strptime('1/21/2025 11:59 PM', '%m/%d/%Y %I:%M %p')



def carts():
    cart_codes = [
        'False',
        'True'
    ]
    random_code = random.choice(cart_codes)
    return random_code


def titles():
    titles_codes = [
        'Best Buy Top Deals',
        'Deal of the Day: Electronics Deals - Best Buy',
        'Member Deals - Best Buy',
        'Clearance Electronics Outlet - Best Buy',
        'Best Buy | Official Online Store | Shop Now &amp; Save',
        'Best Buy Support & Customer Service',
        'Schedule a Service - Best Buy',
        'Remote Support: Geek Squad - Best Buy',
        'Track Your Repair - Best Buy', 'Cart - Best Buy'
    ]
    random_title = random.choice(titles_codes)
    return random_title


def random_usernames():
    usernames = [
        'abagam', 'AmruthaBoyapati', 'Nandy_12', 'NagaLakshmi529', 'AnvitaD23', 'thotaravana3', 'VijayChimata', 'manideepboyapati9', 'sasi2025', 'srikanthpitta18',
        'vikatari','sribigdata24','Shiva005_arch','rtummala999','bhavana916'
    ]
    random_name = random.choice(usernames)
    return random_name

i = 0
while i < 1000:
    numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    visitId = str(uuid.uuid4())
    name = random_usernames()
    cart_items = carts()
    title = titles()
    timestamp = random_date(d1, d2).strftime("%Y-%m-%d %H:%M:%S")


    with open(out_path, "a") as file:
        file.write(json.dumps(title) + "," + json.dumps(visitId) + "," + json.dumps(name.upper()) + "," + json.dumps(cart_items) + "," + json.dumps(timestamp) + "\n")
        i += 1

print("Loop ended")

#2 Using spark submit create a Dataframe and load the data from text file into the Dataframe. Use the first line as header.

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("LoadTextFile") \
    .getOrCreate()
file_path= 'output1.txt'
# Load the data into a DataFrame
df = spark.read.option("header", "true").csv(file_path)

# Show the DataFrame contents
df.show()

#