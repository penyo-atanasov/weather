package com.cba.weather.model;

import java.io.Serializable;

public class Observation implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2677660895141433672L;
	private String iata;
	private String localTime;
	private Long timestamp;
	private Double temp;
	private Double apptemp;
	private Double windspeed;
	private String winddir;
	private Double dewpoint;
	private Double pressure;
	private Double wetbulbtemp;
	private Double raintrace;
	private Double relhum;
	
	public Observation() {
		/* BEAN for creation of datasets - leave EMPTY*/
	}
	
	public Observation(String iata, String localTime, Long timestamp, 
			Double temp, Double apptemp, 
			Double windspeed, String winddir, 
			Double dewpoint, Double pressure,
			Double wetbulbtemp, Double raintrace, 
			Double relhum) {
		this.iata = iata;
		this.localTime = localTime;
		this.timestamp = timestamp;
		this.temp = temp;
		this.apptemp = apptemp;
		this.windspeed = windspeed;
		this.winddir = winddir;
		this.dewpoint = dewpoint;
		this.pressure = pressure;
		this.wetbulbtemp = wetbulbtemp;
		this.raintrace = raintrace;
		this.relhum = relhum;
	}

	public String getIata() {
		return iata;
	}

	public void setIata(String iata) {
		this.iata = iata;
	}

	public String getLocalTime() {
		return localTime;
	}

	public void setLocalTime(String localTime) {
		this.localTime = localTime;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public Double getTemp() {
		return temp;
	}

	public void setTemp(Double temp) {
		this.temp = temp;
	}

	public Double getApptemp() {
		return apptemp;
	}

	public void setApptemp(Double apptemp) {
		this.apptemp = apptemp;
	}

	public Double getWindspeed() {
		return windspeed;
	}

	public void setWindspeed(Double windspeed) {
		this.windspeed = windspeed;
	}

	public String getWinddir() {
		return winddir;
	}

	public void setWinddir(String winddir) {
		this.winddir = winddir;
	}

	public Double getDewpoint() {
		return dewpoint;
	}

	public void setDewpoint(Double dewpoint) {
		this.dewpoint = dewpoint;
	}

	public Double getPressure() {
		return pressure;
	}

	public void setPressure(Double pressure) {
		this.pressure = pressure;
	}

	public Double getWetbulbtemp() {
		return wetbulbtemp;
	}

	public void setWetbulbtemp(Double wetbulbtemp) {
		this.wetbulbtemp = wetbulbtemp;
	}

	public Double getRaintrace() {
		return raintrace;
	}

	public void setRaintrace(Double raintrace) {
		this.raintrace = raintrace;
	}

	public Double getRelhum() {
		return relhum;
	}

	public void setRelhum(Double relhum) {
		this.relhum = relhum;
	}
	
}
