#%%
from numpy import number
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import pandas as pd

# %%
spark = (SparkSession.builder.appName('Test').getOrCreate())

# %%
# Function returns latest state for a type of file by
#  - loading all the deltas
#  - ordering by ExtractDate
#  - taking latest version of a row
#  - and (optionally) removing delete transations

def load_latest_data(df, key_columns, can_delete):
    
    df_by_date = df.withColumn("rn", row_number().over(Window.partitionBy(key_columns).orderBy(col("ExtractDate").desc())))

    df_latest = df_by_date.filter(col("rn") == 1).drop("rn")
    
    if (can_delete):
        df_latest = df_latest.filter(col("DeleteFlag") != 1)
    
    return df_latest

# %%
data_day0 = {
    'key': [1, 2, 3], 
    'data': [10, 20, 30],
    'ExtractDate': [
        datetime(2021,3,1),
        datetime(2021,3,1),
        datetime(2021,3,1)],
    'DeleteFlag': [0, 0, 0]
}

data_day1 = {
    'key': [2, 3], 
    'data': [21, 31],
    'ExtractDate': [
        datetime(2021,3,2),
        datetime(2021,3,2)],
    'DeleteFlag': [0, 0]
}

data_day2 = {
    'key': [2, 3], 
    'data': [22, 31],
    'ExtractDate': [
        datetime(2021,3,3),
        datetime(2021,3,3)],
    'DeleteFlag': [0, 1]
}

data_day3 = {
    'key': [2], 
    'data': [23],
    'ExtractDate': [
        datetime(2021,3,4)],
    'DeleteFlag': [0]
}

incremental_updates = [data_day0, data_day1, data_day2, data_day3]

start_day = 0
end_day = 3

data_directory_name = 'data'

# %%
import os
data_files = os.listdir(data_directory_name)

for file in data_files:
    print(f'Deleting file {file}')
    os.remove(os.path.join(data_directory_name, file))

# %%
for data in incremental_updates[start_day:end_day + 1]:

    pdf = pd.DataFrame(data)
    df = spark.createDataFrame(pdf)
    df.write.mode("append").parquet(data_directory_name)


# %%
df_all = spark.read.parquet(data_directory_name)

# %%
df_all.show()

# %%
df_by_date = df_all.withColumn("rn", row_number().over(Window.partitionBy('key').orderBy(col("ExtractDate").desc())))
df_by_date.show()

# %%
df_latest = df_by_date.filter(col("rn") == 1).drop("rn")
df_latest.show()

# %%
df_latest_deleted = df_latest.filter(col("DeleteFlag") != 1)
df_latest_deleted.show()

# %%
pdf

# %%
df.count()

# %%
df.show(10)


# %%
def test_load_latest_data():
    
    # given
    data = {'key_1': [1, 2, 3, 1, 2, 1], 
            'Data': [10, 20, 30, 11, 21, 12],
            'ExtractDate': [
                datetime(2021,3,1),
                datetime(2021,3,1),
                datetime(2021,3,1),
                datetime(2021,3,2),
                datetime(2021,3,3),
                datetime(2021,3,4)],
            'DeleteFlag': [0, 0, 0, 0, 0, 1]
            }

    pdf = pd.DataFrame(data)
    df = spark.createDataFrame(pdf)

    # when
    output = load_latest_data(df, ['key_1'], False)
    output_pdf = output.toPandas()

    # then
    assert output.count() == 3
    output_pdf.loc[output_pdf['key_1'] == 1, 'data'] == 12
    output_pdf.loc[output_pdf['key_1'] == 2, 'data'] == 21
    output_pdf.loc[output_pdf['key_1'] == 3, 'data'] == 30
