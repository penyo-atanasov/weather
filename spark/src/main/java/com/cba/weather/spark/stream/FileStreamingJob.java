package com.cba.weather.spark.stream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.cba.weather.model.Station;
import com.cba.weather.spark.Configuration;

public class FileStreamingJob extends BaseStreamingJob {

	private String inputPath;

	public FileStreamingJob(String inputPath, String observationsStoragePath, 
			String stationsStoragePath, int batchDurationSeconds, 
			Map<String, Station> stations) {
		super(observationsStoragePath, stationsStoragePath, batchDurationSeconds, stations);
		this.inputPath = inputPath;
	}

	@Override
	protected JavaDStream<String> createDStream(JavaStreamingContext context) {
		return context.textFileStream("file://" + this.inputPath);
	}
	

	public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {
		Configuration conf = Configuration.get(args);
		FileStreamingJob job = new FileStreamingJob(
				conf.getFileStreamingPath(), 
				conf.getStationsStoragePath(),
				conf.getObservationsStoragePath(), 
				conf.getBatchDurationSeconds(), 
				conf.getStations());
		job.start();
	}
	
}
