import string
import numpy as np
import pandas as pd
import random
import os
import shutil
import datetime as dt
from tqdm import tqdm

random.seed(0)

def gen_text(number_of_item, long_of_text, text_list):
    item_list = []
    while len(item_list) < number_of_item :
        item_name = ''.join(random.choices(text_list, k = long_of_text))
        if item_name not in item_list:
            item_list.append(item_name)
    return item_list

folder_name = 'data_sample'
if  folder_name in os.listdir() :
    shutil.rmtree(folder_name)
os.mkdir(folder_name)

list_date = pd.date_range("2023-01-01", end='2023-01-31', freq="min")
print('list_date : ',len(list_date))
list_char = list(string.ascii_lowercase)
column_template = ['department_name','sensor_serial','create_at','product_name','product_expire']
number_of_department = 100
sensor_in_department = [random.choices(range(5,30))[0] for i in range(number_of_department)]
number_of_sensor = sum(sensor_in_department)
number_of_product = 1000
department_list = gen_text(number_of_item = number_of_department, long_of_text = 32, text_list = list_char)
sensor_list = gen_text(number_of_item = number_of_sensor, long_of_text = 64, text_list = list_char)
product_list = gen_text(number_of_item = number_of_product, long_of_text = 16, text_list = list_char)

data_template = pd.DataFrame(columns=['department_name','sensor_serial'])
for i in range(number_of_department):
    d_name = department_list[i]
    n_sensor = sensor_in_department[i]
    check_list = list(data_template['sensor_serial'].unique())
    a = [s for s in sensor_list if s not in check_list]
    data_department = pd.DataFrame({
        'department_name' : [d_name] * n_sensor
        , 'sensor_serial' : random.choices(
            [s for s in sensor_list if s not in check_list]
            , k = n_sensor
            )
        })
    data_template = pd.concat([data_template,data_department], ignore_index=True)
del data_department

for i in tqdm(range(len(list_date))):
    data_date = data_template.copy()
    create_at = list_date[i]
    path = os.path.join(folder_name,str(create_at)) + '.parquet'
    data_date['create_at'] = create_at
    data_date['product_name'] = random.choices(product_list, k = number_of_sensor)
    data_date['product_expire'] = list(map(lambda x : x + dt.timedelta(days = 90 - random.choices([1,2,3])[0]), data_date['create_at']))
    data_date.to_parquet(path)