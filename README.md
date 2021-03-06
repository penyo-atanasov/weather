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
 * Current results with 16 stations and 144 observations per station from the past 72 hours using LinearRegression:

```
Pressure
RMSE: 2.9424744341420745
r2: 0.4882222112647927

Humidity
RMSE: 5.631818845224078
r2: 0.9612451568162376

Temperature
RMSE: 1.0655829295792203
r2: 0.9853273572257101
```

## Howto
 * Enter the directory before executing the following scripts.
 * Use install-spark.sh to setup the env
 * Use build.sh to build the modules. Requirements:
    * Python 2.7
	* Maven
	* Java 1.8
 * Use start.sh to start the data feed and the spark streaming application
    * logs/ dir contains streaming.log and datafeed.log
	* use stop.sh to stop the streaming and feeding.
 * Use predict.sh to generate the actual predictions.

## Data
![Observations](https://github.com/penyo-atanasov/weather/blob/master/data_observations.png "Observations")

![Stations](https://github.com/penyo-atanasov/weather/blob/master/data_stations.png "Stations")


 
## Considerations
 * Add real spark streaming through a large scale messaging and eventually remove the Socker Server
 * Tweak the overwrite/append modes and aggregate more data without duplications
 * Check other features and features set and test different models
 * Add more stations and data feeds
 * etc etc




