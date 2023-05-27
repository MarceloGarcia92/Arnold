from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, to_date

app_name = 'Strava_ETL'
request = 'activities'

print('Setting Spark')
spark = SparkSession \
        .builder \
        .appName(app_name) \
        .getOrCreate()

print(f'strava application: {app_name}. Successfully loaded') 
class Activities():
        def top_activities(self):
                df = spark.read.json(f'{request}/strava_{request}.json')

                print('Activity data have been loaded')
                df = df.withColumn('Date', regexp_extract(df.start_date_local, r'[0-9]{4}-[0-9]{2}-[0-9]{2}', 0))
                #df = df.withColumn('Date', to_date(df.start_date_local, 'MM-dd-yyyy'))
                clean_df = df.select('id', 'name', 'distance', 'moving_time', 'total_elevation_gain', 'sport_type', 'Date', 'end_latlng')

                print(f'Saving tabla: {request}')        
                clean_df.createOrReplaceTempView(request)

                try:
                        print(f'Saving parquet file of {request}')
                        clean_df.write.parquet(f"{request}/{request}.parquet")

                except:
                        print('parquet already exists')

                print(f'Schema of tabla: {request}')
                clean_df.printSchema()

                print(f'Data example of tabla: {request}')
                clean_df.show(5)

                print(f'Top 5 {request}')
                spark.sql(f"""SELECT 
                                sport_type, 
                                count(*)
                        FROM {request}
                        GROUP BY sport_type
                        ORDER BY COUNT(*) DESC""").show()

                

                

