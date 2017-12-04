package com.cba.weather.model;

public enum Condition {
	
	RAIN("RAIN"),
	SNOW("SNOW"),
	SUNNY("SUNNY");
	
	private String toString;

	private Condition(String toString) {
		this.toString = toString;
	}
	
	@Override
	public String toString() {
		return toString;
	}
	
	public static Condition get(double temperature, double humidity) {
		if(temperature < 0 && humidity < 30) {
			return SNOW;
		} else if (humidity > 70) {
			return RAIN;
		}
		return SUNNY;
	}
	
}
