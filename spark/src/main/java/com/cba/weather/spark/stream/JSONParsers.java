package com.cba.weather.spark.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONArray;
import org.json.JSONObject;

import com.cba.weather.model.Observation;
import com.cba.weather.model.Station;

public class JSONParsers{
	
	public static final List<Observation> parseBureauOfMeteorology(String jsonStr, Map<String, Station> stations) {
		List<Observation> obs = new ArrayList<>();
		if (jsonStr.length() > 0) {
			System.out.println(jsonStr);
			JSONObject json = new JSONObject(jsonStr);
			JSONObject observations = json.getJSONObject("observations");
			JSONObject header = observations.getJSONArray("header").getJSONObject(0);
			String name = header.getString("name");
			Station station = stations.get(name);
			JSONArray data = observations.getJSONArray("data");
			for (int i = 0; i < data.length(); i++) {
				JSONObject item = data.getJSONObject(i);
				DateTimeFormatter parse = DateTimeFormat.forPattern("yyyyMMddHHmmss");
				DateTimeFormatter print = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
				String localTime = print.print(parse.parseDateTime(item.getString("local_date_time_full")));
				Long utcTimeStamp = parse.parseDateTime(item.getString("aifstime_utc")).getMillis();
				Double temp = item.getDouble("air_temp");
				Double apptemp = item.optDouble("apparent_t");
				Double windspeed = item.optDouble("wind_spd_kmh", 0);
				Double wetbulbtemp = item.optDouble("delta_t");
				String rainTraceStr = item.getString("rain_trace");
				Double raintrace = new Double(0);
				if(!rainTraceStr.equals("-")) {
					raintrace = Double.parseDouble(rainTraceStr);
				}
				String winddir = item.getString("wind_dir");
				Double dewpoint = item.optDouble("dewpt");
				Double relhum = item.optDouble("rel_hum");
				Double pressure = item.getDouble("press");
				if(!apptemp.equals(Double.NaN)
						&& !windspeed.equals(Double.NaN)
						&& !wetbulbtemp.equals(Double.NaN)
						&& !raintrace.equals(Double.NaN)
						&& !dewpoint.equals(Double.NaN)
						&& !relhum.equals(Double.NaN)) {
					Observation o = new Observation(station.getIata(), localTime, utcTimeStamp, temp, apptemp, windspeed, winddir, dewpoint, pressure,
							wetbulbtemp, raintrace, relhum);
					obs.add(o);
				}
			}
		}
		System.out.println(obs);
		
		return obs;
	}
	
}
