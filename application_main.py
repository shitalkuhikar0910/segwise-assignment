import sys
from lib import DataManipulation, DataReader, Utils, DataWriter
from pyspark.sql.functions import *
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)
    job_run_env = sys.argv[1]
    print("Creating Spark Session")
    spark = Utils.get_spark_session(job_run_env)
    print("Created Spark Session")
    app_df = DataReader.read_app_data(spark,job_run_env)
    filtered_app_df = DataManipulation.valid_year_df(app_df)
    
    
    #Free apps genre wise with launched year
    free_genre_ryear_df = filtered_app_df.groupBy("isfree","genre","released_year").count()       
    concatenated_df = DataManipulation.concatenation(free_genre_ryear_df)
    concatenated_df.show(truncate=False)
        
    
    #Apps between certain price range, released in certain year, is ad supported and has certain rating
    price_ad_rating_df = filtered_app_df.groupBy("price","released_year","ad_support","score").count()
    concatenated_df2 = DataManipulation.concatenation(price_ad_rating_df)
    concatenated_df2.show(truncate=False)
    
    
    # Top Free Apps    
    top_free_app_df = filtered_app_df.select("appid","developer","genre","min_install").where("isfree=1").sort("min_install",ascending=False).limit(100)
    concatenated_df3 = DataManipulation.concatenation(top_free_app_df)    
    concatenated_df3.show(truncate=False)
    
    
    # Top deveopers
    top_developer_df = filtered_app_df.select("developer","score").sort("score",ascending=False).limit(100)
    top_developer_df.show()
    
    
    # Popular genre
    popular_genre_df = filtered_app_df.groupBy("genre").count().sort("count",ascending=False).limit(100)
    popular_genre_df.show()
    
    
    #Highest Reviews app
    highest_review_df = filtered_app_df.select("appid","developer","reviews").sort("reviews",ascending=False).limit(100)
    highest_review_df.show()
    
    
    # Apps which are not updated in last 6 months    
    from datetime import datetime,timedelta
    current_date = datetime.now()
    six_months = current_date - timedelta(days=180)
    formatted_updated_date_df =  filtered_app_df.withColumn("date_updated", to_timestamp("date_updated", "dd-MM-yyyy HH:mm"))
    not_updated_apps_df = formatted_updated_date_df.select("appid","developer","date_updated").filter(col("date_updated")<=six_months)
    not_updated_apps_df.show()
    
    
    #Standard release day
    standard_release_date_df = filtered_app_df.withColumn(
    "st_released_date",
    to_date(unix_timestamp(col("released_day_year"), "MMM d, yyyy").cast("timestamp"))
    ).withColumn("day_of_week",date_format(col("st_released_date"), "E"))
    most_apps_released_on_df = standard_release_date_df.select("day_of_week").groupBy("day_of_week").count().sort("count",ascending=False)
    most_apps_released_on_df.show()