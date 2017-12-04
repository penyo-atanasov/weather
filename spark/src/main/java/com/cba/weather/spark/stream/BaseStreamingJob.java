package com.cba.weather.spark.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.cba.weather.model.Observation;
import com.cba.weather.model.Station;
import com.cba.weather.spark.BaseSparkJob;

import scala.Tuple2;

public abstract class BaseStreamingJob extends BaseSparkJob {

	private String observationsStoragePath;
	private String stationsStoragePath;
	private int batchDurationSeconds;
	private Map<String, Station> stations;
	
	public BaseStreamingJob(String observationsStoragePath, String stationsStoragePath, int batchDurationSeconds, Map<String, Station> stations) {
		this.observationsStoragePath = observationsStoragePath;
		this.stationsStoragePath = stationsStoragePath;
		this.batchDurationSeconds = batchDurationSeconds;
		this.stations = stations;
	}
	
	@Override
	protected void start() throws InterruptedException {
		JavaStreamingContext streamingContext = new JavaStreamingContext(javaContext(), new Duration(1000L * batchDurationSeconds));
		SQLContext sqlContext = sqlContext();
		//persist stations if new ones arrive
		List<Station> persist = new ArrayList<>();
		persist.addAll(stations.values());
		Dataset<Row> stationsOverwrite = sqlContext.createDataFrame(persist, Station.class);
		stationsOverwrite.write().mode(SaveMode.Overwrite).parquet(stationsStoragePath);
		
		//broadcast the stations for IATA retrieval from custom JSONs coming from the stream
		Broadcast<Map<String, Station>> stationsBr = javaContext().broadcast(stations);
		JavaDStream<String> lines = createDStream(streamingContext);
		lines.foreachRDD(line -> {
			JavaPairRDD<String, List<Observation>> pairRdd = line.mapToPair(l -> {
				List<Observation> obs = JSONParsers.parseBureauOfMeteorology(l, stationsBr.getValue());
				if(obs.size() > 0) {
					return new Tuple2<>(obs.get(0).getIata(), obs);
				} else {
					return new Tuple2<>("", obs);
				}
			}).filter(t -> {
				return !t._1().equals("");
			});
			Map<String, List<Observation>> dataMap = pairRdd.collectAsMap();
			for (Entry<String, List<Observation>> e : dataMap.entrySet()) {
				Dataset<Row> data = sqlContext.createDataFrame(e.getValue(), Observation.class);
				data.write().mode(SaveMode.Append).parquet(observationsStoragePath + "/" + e.getKey());
			}
		});
		streamingContext.start();
		streamingContext.awaitTermination();
		streamingContext.close();
	}
	
	protected abstract JavaDStream<String> createDStream(JavaStreamingContext context);
	
}
