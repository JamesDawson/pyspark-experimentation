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
    output_pdf.loc[output_pdf['key_1'] == 1, 'Data'] == 12
    output_pdf.loc[output_pdf['key_1'] == 2, 'Data'] == 21
    output_pdf.loc[output_pdf['key_1'] == 3, 'Data'] == 30

# %%
test_load_latest_data()
