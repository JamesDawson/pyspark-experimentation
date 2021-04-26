#%%
from pyspark.sql import SparkSession
from datetime import datetime
import pandas as pd
from data_loading import ProcessDeltas

# %%
spark = (SparkSession.builder.appName('Test').getOrCreate())

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
    output = ProcessDeltas.load_latest_data(df, ['key_1'], False)
    output_pdf = output.toPandas()

    # then
    assert output.count() == 3
    output_pdf.loc[output_pdf['key_1'] == 1, 'Data'] == 12
    output_pdf.loc[output_pdf['key_1'] == 2, 'Data'] == 21
    output_pdf.loc[output_pdf['key_1'] == 3, 'Data'] == 30
