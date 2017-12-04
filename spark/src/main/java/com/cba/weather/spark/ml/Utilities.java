package com.cba.weather.spark.ml;

import org.apache.ivy.Main;

public class Utilities {


	private static final double UNIVERSAL_GAS_CONSTANT = 8.3144598;
	private static final double GRAVITATIONAL_ACCELERATION = 9.80665;
	private static final double MOLAR_MASS = 0.0289644;
	private static final double KELVIN_OFFSET = 273.15;
	private static final double STANDARD_TEMPERATURE = KELVIN_OFFSET + 15;
	private static final double STATIC_PRESSURE = 101325.0; 
	
	  public static final double TEMPERATURE_LAPSE = 0.0065;
	  private static final double STANDARD_PRESSURE = 101325.0;
	  private static final double KELVIN = 273.15;
	  private static final double GAS_CONSTANT = 8.31447;
	  private static final double MOLAR = 0.0289644;
	  private static final double GRAVITY = 9.80665;

	  public static double calculate(double altitudeInMeters, double temperatureInCelsius) {
	    double temperatureInKelvin = KELVIN + temperatureInCelsius;

	    double standardPressureWithAltitude = barometricFormula(altitudeInMeters, temperatureInKelvin);

	    double pressureWithTemperature = (standardPressureWithAltitude * STANDARD_TEMPERATURE / temperatureInKelvin);

	    return toHPa(pressureWithTemperature);
	  }

	  private static double barometricFormula(double altitudeInMeters, double temperatureInKelvin) {
	    return STANDARD_PRESSURE * Math.pow((1 - (TEMPERATURE_LAPSE * altitudeInMeters / STANDARD_TEMPERATURE)), (GRAVITY * MOLAR) / (GAS_CONSTANT * TEMPERATURE_LAPSE));
	  }

	  private static double toHPa(double pressureInPa) {
	    return Math.round(pressureInPa) / 100;
	  }
	
	public static double calculatePressure(double height, double tempC) {
		double exp = ((-GRAVITATIONAL_ACCELERATION * MOLAR_MASS * height) / UNIVERSAL_GAS_CONSTANT * STANDARD_TEMPERATURE);
		double press = STATIC_PRESSURE * Math.exp(exp);
		double pressWithTemp = (press * STANDARD_TEMPERATURE)/ (KELVIN_OFFSET + tempC); //pa
		return pressWithTemp; //convert to hpa
	}
	
	public static void main(String[] args) {
//		12.1,1010.9
		
		double d = calculate(39.0,  21.8);//calculatePressure(39.0, 12.1);
		System.out.println(d);
		System.out.println(String.format("%.2f", d));
	}
	
}
