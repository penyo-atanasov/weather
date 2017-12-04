# Weather Prediction System POC
## Modules
### Datafeeder
 * A simple Socker Server App written in Python serving feeding data to a Spark Streaming application.
 * The datafeeder get data from Bureau of Meteorology (Australia) given a csv with stations data and endpoint JSON urls.
 * All the observations are sent to Spark except the newest one which is to be predicted.
 * The server queries data every 30 minutes.

### Spark Streaming
 * Aggregates data into parquet files.
 * Observations data - data/observations/<IATA> 
 * Stations data  - data/stations
 * Currently the system uses the SocketStreamingJob. There is also a FileStreamingJob implemented in case larger JSON are provided.

### Spark ML
 * Features - lat,lon,height,dewpoint,raintrace,timestamp,wetbulb,windspeed
 * Uses 3 Logistion Regression models to predict Temperature, Pressure and Humidity all of which use the same features.
 * Condition is predicted based on predicted temperature and humidity.
 * Output the predictions in data/predictions/predictions.csv
 * Reads the feature data from a csv file in data/predictions/<PATH>
	 
## Howto
 * Enter the directory before executing the following scripts.
 * Use install-spark.sh to setup the env
 * Use build.sh to build the modules. Python 2.7 requred.
 * Use start.sh to start the data feed and the spark streaming application
    * logs/ dir contains streaming.log and datafeed.log
	* use stop.sh to stop the streaming and feeding.
 * Use predict.sh to generate the actual predictions.
 
## Considerations
 * Add real spark streaming through a large scale messaging (Kafka)
 * Tweak the overwrite/append modes and aggregate more data without duplications
 * Check other features and features set and test different models
 * Add more stations and data feeds
 * etc etc

