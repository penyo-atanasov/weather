package com.cba.weather.spark.stream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.cba.weather.model.Station;
import com.cba.weather.spark.Configuration;

public class SocketStreamingJob extends BaseStreamingJob {

	private String socketHost;
	private int socketPort;
	
	public SocketStreamingJob(String socketHost, int socketPort, 
			String observationsStoragePath, String stationsStoragePath, 
			int batchDurationSeconds, Map<String, Station> stations) {
		super(observationsStoragePath, stationsStoragePath, batchDurationSeconds, stations);
		this.socketHost = socketHost;
		this.socketPort = socketPort;
	}

	@Override
	protected JavaDStream<String> createDStream(JavaStreamingContext context) {
		return context.socketTextStream(socketHost, socketPort, StorageLevel.MEMORY_ONLY());
	}
	
	public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {
		Configuration conf = Configuration.get(args);
		SocketStreamingJob job = new SocketStreamingJob(
				conf.getSocketHost(), conf.getSocketPort(), 
				conf.getObservationsStoragePath(), 
				conf.getStationsStoragePath(), 
				conf.getBatchDurationSeconds(), 
				conf.getStations());
		job.start();
	}

}
