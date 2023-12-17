from pyspark.sql.functions import *

def valid_year_df(app_df):
    return app_df.where("released_year<2050 and released_year>=2000")

def concatenation(final_df):
    return final_df.select(
        concat_ws(';', 
              *[concat_ws('=', lit(col), col) for col in final_df.columns[:-1]]
             ).alias("concatenated_key"),final_df.columns[-1])