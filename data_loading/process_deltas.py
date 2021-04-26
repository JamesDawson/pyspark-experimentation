#%%
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# %%
# Function returns latest state for a type of file by
#  - loading all the deltas
#  - ordering by ExtractDate
#  - taking latest version of a row
#  - and (optionally) removing delete transations

class ProcessDeltas:

    @staticmethod
    def load_latest_data(df, key_columns, can_delete):
        
        df_by_date = df.withColumn("rn", row_number().over(Window.partitionBy(key_columns).orderBy(col("ExtractDate").desc())))

        df_latest = df_by_date.filter(col("rn") == 1).drop("rn")
        
        if (can_delete):
            df_latest = df_latest.filter(col("DeleteFlag") != 1)
        
        return df_latest
