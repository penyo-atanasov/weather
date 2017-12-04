package com.cba.weather.spark;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.cba.weather.model.Station;

public class Configuration {

	private String observationsStoragePath;
	private String stationsStoragePath;
	private String predictionsStoragePath;
	private String fileStreamingPath;
	private String socketHost;
	private int socketPort = Integer.MIN_VALUE;
	private int batchDurationSeconds = Integer.MIN_VALUE;
	private Map<String, Station> fullNameToStation;

	public static Configuration get(String[] args) throws FileNotFoundException, IOException {
		Configuration conf = new Configuration();
		conf.load(args);
		return conf;
	}

	private Configuration() {
		// empty;
	}

	private void load(String[] args) throws FileNotFoundException, IOException {
		Properties props = loadProps("conf/storage.props");
		this.observationsStoragePath = props.getProperty("data.observations.path");
		this.stationsStoragePath = props.getProperty("data.stations.path");
		this.predictionsStoragePath = props.getProperty("data.predictions.path");
		props = loadProps("conf/streaming.props");
		this.fileStreamingPath = props.getProperty("file.streaming.path");
		this.socketHost = props.getProperty("socket.streaming.host");
		String strPort = props.getProperty("socket.streaming.port");
		if(strPort != null) {
			this.socketPort = Integer.valueOf(strPort);
		}
		String durationSeconds = props.getProperty("batch.duration.seconds");
		if(durationSeconds!=null) {
			this.batchDurationSeconds = Integer.valueOf(durationSeconds);
		}
		
		try(FileReader reader = new FileReader(new File("conf/iata.txt"))) {
			CSVParser parser = CSVParser.parse(reader, CSVFormat.DEFAULT);
			this.fullNameToStation = new HashMap<>();
			for (CSVRecord csvRecord : parser) {
				String name = csvRecord.get(0);
				String iata = csvRecord.get(1);
				String fullName = csvRecord.get(2);
				Double lat = Double.valueOf(csvRecord.get(3));
				Double lon = Double.valueOf(csvRecord.get(4));
				Double height = Double.valueOf(csvRecord.get(5));
				Station station = new Station(iata, name, fullName, lat, lon, height);
				fullNameToStation.put(fullName, station);
			}
		}
    	
	}

	private Properties loadProps(String path) throws FileNotFoundException, IOException {
		Properties props = new Properties();
		props.load(new FileInputStream(new File(path)));
		return props;
	}

	public String getObservationsStoragePath() {
		return observationsStoragePath;
	}

	public String getPredictionsStoragePath() {
		return predictionsStoragePath;
	}

	public String getFileStreamingPath() {
		return fileStreamingPath;
	}

	public String getSocketHost() {
		return socketHost;
	}

	public int getSocketPort() {
		return socketPort;
	}

	public int getBatchDurationSeconds() {
		return batchDurationSeconds;
	}
	
	public Map<String, Station> getStations() {
		return fullNameToStation;
	}
	
	public String getStationsStoragePath() {
		return stationsStoragePath;
	}
}
