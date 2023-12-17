from lib import ConfigReader

#defining customers schema
def get_app_schema():
    app_schema = 'appid string, developer string, isfree int, genre string, genreid string, in_app_product_price string, min_install integer, price float, ratings integer, len_screenshot integer, ad_support integer, contains_add integer, reviews integer, released_day_year string, sale integer, score float, released_day integer, released_year integer, released_month string, date_updated string'
    return app_schema


# creating customers dataframe
def read_app_data(spark,env):
    conf = ConfigReader.get_app_config(env)
    app_data_file_path = conf["app_data.file.path"]
    return spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(get_app_schema()) \
    .load(app_data_file_path)
