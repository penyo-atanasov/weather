package com.cba.weather.model;

import java.io.Serializable;

public class Station implements Serializable {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1149564458212021771L;
	private String iata;
	private String name;
	private String fullName;
	private Double lat;
	private Double lon;
	private Double height;

	public Station() {
		/* BEAN for creation of datasets - leave EMPTY*/
	}
	
	public Station(String iata, String name, String fullName, Double lat, Double lon, Double height) {
		this.iata = iata;
		this.name =  name;
		this.fullName = fullName;
		this.lat = lat;
		this.lon = lon;
		this.height = height;
	}

	public String getIata() {
		return iata;
	}

	public void setIata(String iata) {
		this.iata = iata;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFullName() {
		return fullName;
	}

	public void setFullName(String fullName) {
		this.fullName = fullName;
	}

	public Double getLat() {
		return lat;
	}

	public void setLat(Double lat) {
		this.lat = lat;
	}

	public Double getLon() {
		return lon;
	}

	public void setLon(Double lon) {
		this.lon = lon;
	}

	public Double getHeight() {
		return height;
	}

	public void setHeight(Double height) {
		this.height = height;
	}
	
}
