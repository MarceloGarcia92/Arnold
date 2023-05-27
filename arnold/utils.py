from geopy.geocoders import Nominatim
from arnold.ETL import spark
from os import remove
from shutil import rmtree

from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import ArrayType, StringType

def find_location():
    df=spark.read.parquet("activities/activities.parquet")
    list_latlong = df.rdd.map(lambda x: x[7]).collect()
    Country = list()

    for latlong in list_latlong:
        print(f'Processing the new activity location: {latlong}')
        geolocator = Nominatim(user_agent="http")
        location = geolocator.geocode(latlong, addressdetails=True)
        try:
            address = location.raw['address']
            country = address.get('country', '')
            Country.append(country)
            print(f'Found new activity location: {country}')

        except:
            print(f'Didnt find activity location')
            Country.append('')

    #rmtree('activities/activities.parquet')

    # UDF to convert list to column
    list_to_column_udf = udf(lambda x: x, ArrayType(StringType()))

# Add new column using the UDF
    final_df = df.withColumn("new_column", list_to_column_udf(Country))

    #final_df = df.withColumn("Country", lit(country for country in Country))
    final_df.show()
