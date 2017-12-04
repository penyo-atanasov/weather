package com.cba.weather.spark.ml;

import static org.apache.spark.sql.functions.col;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.cba.weather.model.Condition;
import com.cba.weather.spark.BaseSparkJob;
import com.cba.weather.spark.Configuration;

import scala.Tuple2;
import scala.Tuple3;

public class LearningJob extends BaseSparkJob {

	private static final String JOIN_COL = "iata";
	private static final String ORDER_COL = "timestamp";
	private static final String FEATURES = "features";
	private static final String LABEL = "label";
	private static final String PREDICTION = "prediction";
	private static final String ONE_DECIMAL_POINT = "%.1f";
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		Configuration conf = Configuration.get(args);
		LearningJob learn = new LearningJob(conf.getObservationsStoragePath(), conf.getStationsStoragePath(), conf.getPredictionsStoragePath());
		learn.start();
		
	}

	private String predictionsStoragePath;
	private String observationsStoragePath;
	private String stationsStoragePath;

	public LearningJob(String observationsStoragePath, String stationsStoragePath, String predictionsStoragePath) {
		this.observationsStoragePath = observationsStoragePath;
		this.predictionsStoragePath = predictionsStoragePath;
		this.stationsStoragePath = stationsStoragePath;
	}

	
	public void start() {
		SQLContext sqlContext = sqlContext();
		
//		lat,lon,height,dewpoint,raintrace,timestamp,wetbulb,windspeed
		JavaRDD<Row> javaRdd = 
				javaContext().textFile(this.predictionsStoragePath + "/test.csv").map(line -> {
					String[] metrics = line.trim().split(",");
					Double lat = Double.valueOf(metrics[0]);
					Double lon = Double.valueOf(metrics[1]);
					Double height = Double.valueOf(metrics[2]);
					Double dewpoint = Double.valueOf(metrics[3]);
					Double raintrace = Double.valueOf(metrics[4]);
					Long timestamp = Long.valueOf(metrics[5]);
					Double wetbulb = Double.valueOf(metrics[6]);
					Double windspeed = Double.valueOf(metrics[7]);
					return RowFactory.create(lat, lon, height, dewpoint, raintrace, timestamp, wetbulb, windspeed);			
				});
		StructType schema = DataTypes.createStructType(new StructField[]{
				new StructField("lat", DataTypes.DoubleType, false, Metadata.empty()),
				new StructField("lon", DataTypes.DoubleType, false, Metadata.empty()),
				new StructField("height", DataTypes.DoubleType, false, Metadata.empty()),
				new StructField("dewpoint", DataTypes.DoubleType, false, Metadata.empty()),
				new StructField("raintrace", DataTypes.DoubleType, false, Metadata.empty()),
				new StructField("timestamp", DataTypes.LongType, false, Metadata.empty()),
				new StructField("wetbulbtemp", DataTypes.DoubleType, false, Metadata.empty()),
				new StructField("windspeed", DataTypes.DoubleType, false, Metadata.empty())});
		Dataset<Row> test = sqlContext.createDataFrame(javaRdd.rdd(), schema);
//		test.show();
		Set<String> predictValues = new HashSet<>();
		predictValues.addAll(Arrays.asList(new String[]{
			"pressure", "apptemp", "relhum"	
		}));
		//not fair to use temp in order to predict apptemp. When using temp the RMSE is < 0.3
		Set<String> excludeValues = new HashSet<>();
		excludeValues.addAll(Arrays.asList(new String[]{
			"temp"
		}));
		
		Map<String, List<String>> labelToFeatures = new HashMap<>();
		
		Dataset<Row> stations = sqlContext.read()
				.parquet(this.stationsStoragePath);
		Dataset<Row> observations = sqlContext.read()
				.parquet(this.observationsStoragePath + "/*");
		
		Dataset<Row> data = observations.join(stations, 
				observations.col(JOIN_COL).equalTo(stations.col(JOIN_COL)), "left_outer");
        //add the columns used to train
		for(String label : predictValues) {
			List<String> cols = new ArrayList<>();
			for(StructField field : schema.fields()) {
				if(!excludeValues.contains(field.name())
						&& !predictValues.contains(field.name()) 
						&& (field.dataType().equals(DataTypes.DoubleType) 
								|| field.dataType().equals(DataTypes.LongType))) {
					cols.add(field.name());
				}
			}
			labelToFeatures.put(label, cols);
		}
		
		Map<String, Dataset<Row>> predictions = trainPredict(labelToFeatures, sqlContext, data, test);
		
		predictions.get("apptemp").show();
		predictions.get("pressure").show();
		predictions.get("relhum").show();
		
		JavaPairRDD<String, Double> tempPreds = computeLocationToValueAsJavaRDD(predictions.get("apptemp"));
		JavaPairRDD<String, Double> pressPreds = computeLocationToValueAsJavaRDD(predictions.get("pressure"));
		JavaPairRDD<String, Double> humPreds = computeLocationToValueAsJavaRDD(predictions.get("relhum"));
		
		
		JavaPairRDD<String, Tuple3<Double, Double, Double>> predsCombined = 
				tempPreds.join(pressPreds.join(humPreds)).mapToPair(tup -> {
					String key = tup._1();
					Double temp = tup._2()._1();
					Double press = tup._2()._2()._1();
					Double relhum = tup._2()._2()._2();
					return new Tuple2<>(key, new Tuple3<>(temp, press, relhum));
				});
		Map<String, Tuple3<Double, Double, Double>> predictionsMap = predsCombined.collectAsMap();
		
		JavaPairRDD<String, String> locationToIATA = 
				stations.select("lat", "lon", "height", "iata").toJavaRDD().mapToPair(r ->{
					String key = getLocationAsString(r);
					String iata = r.getString(3);
					return new Tuple2<>(key, iata);
				});
		
		Map<String, String> locationMap = locationToIATA.collectAsMap();
		
		DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
		try (CSVPrinter csvPrinter = new CSVPrinter(
				new FileWriter(this.predictionsStoragePath + "/predictions.csv"), 
				CSVFormat.DEFAULT.withDelimiter('|'))) {
			for(Entry<String, Tuple3<Double,Double,Double>> predictionE : predictionsMap.entrySet()) {
				String iata = locationMap.get(predictionE.getKey());
				String location = predictionE.getKey();
				Tuple3<Double,Double,Double> tempPressRelhum = predictionE.getValue();
				Double temp = tempPressRelhum._1();
				Double press = tempPressRelhum._2();
				Double relhum = tempPressRelhum._3();
				String time = formatter.print(DateTime.now(DateTimeZone.UTC));
				String condition = Condition.get(temp, relhum).toString();
				String tempSign = (temp > 0 ? "+" : (temp < 0 ? "-" : ""));
				
				String tempStr = tempSign + String.format(ONE_DECIMAL_POINT, temp);
				String pressStr = String.format(ONE_DECIMAL_POINT, press);
				String humStr = Long.toString(Math.round(relhum));
				
				csvPrinter.print(iata);
				csvPrinter.print(location);
				csvPrinter.print(time);
				csvPrinter.print(condition);
				csvPrinter.print(tempStr);
				csvPrinter.print(pressStr);
				csvPrinter.print(humStr);
				csvPrinter.println();
			}
			csvPrinter.flush();
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
	}
	
	private static JavaPairRDD<String, Double> computeLocationToValueAsJavaRDD(Dataset<Row> predictions) {
		JavaPairRDD<String, Double> preds = 
				predictions.select("lat", "lon", "height", PREDICTION).toJavaRDD().mapToPair(r -> {
					String key = getLocationAsString(r);
					Double pred = r.getDouble(3);
					return new Tuple2<>(key, pred); 
				});
		return preds;
	}
	
	private static String getLocationAsString(Row row) {
		Double lat = row.getDouble(0);
		Double lon = row.getDouble(1);
		Double height = row.getDouble(2);
		String key = String.format("%f,%f,%f", lat, lon, height);
		return key;
	}
	
	private Map<String, Dataset<Row>> trainPredict(Map<String, List<String>> labelToFeatures, SQLContext sqlContext, Dataset<Row> data, Dataset<Row> predict) {
		Map<String, Dataset<Row>> predictions = new HashMap<>();
		
		for(Entry<String, List<String>> e : labelToFeatures.entrySet()) {
			List<String> columns = new ArrayList<>();
			columns.add(e.getKey()); //label columns goes first
			List<String> features = e.getValue();
			columns.addAll(features);
			Column [] cols = new Column[columns.size()];
			cols[0] = new Column(columns.get(0)).as(LABEL);
			for (int i = 1; i < columns.size(); i++) {
				cols[i] = new Column(columns.get(i));
			}
			
			//select feature columns
			Dataset<Row> subset = data.select(cols).orderBy(col(ORDER_COL));

			VectorAssembler featuresAssembler = new VectorAssembler().setInputCols(
					columns.subList(1, columns.size()).toArray(new String[columns.size()-1]))
					.setOutputCol(FEATURES);
			subset = featuresAssembler.transform(subset);
			
//			Dataset<Row>[] splits = subset.randomSplit(new double[]{0.9, 0.1}, 1234L);
//			Dataset<Row> train = splits[0];
//			Dataset<Row> test = splits[1];
			
			
			LinearRegression lr = 
					new LinearRegression().setMaxIter(50).setRegParam(0.025); 
			LinearRegressionModel model = lr.train(subset);
			Column[] predictCols = new Column[features.size()];
			for (int i = 0; i < features.size(); i++) {
				predictCols[i] = new Column(features.get(i));
			}
			
			Dataset<Row> predictTransformed = predict.select(predictCols).orderBy(col(ORDER_COL)); 
			featuresAssembler = new VectorAssembler().setInputCols(
					features.toArray(new String[features.size()]))
					.setOutputCol(FEATURES);
			predictTransformed = featuresAssembler.transform(predictTransformed);
			
//			model.transform(test);
//			LinearRegressionTrainingSummary summary = model.summary();
//		    System.out.println("numIterations: " + summary.totalIterations() + "\n");
//		    System.out.println("objectiveHistory: " + Vectors.dense(summary.objectiveHistory()) + "\n");
//		    System.out.println("RMSE: " + summary.rootMeanSquaredError() + "\n");
//		    System.out.println("r2: " + summary.r2() + "\n");

			Dataset<Row> result = model.transform(predictTransformed);
			predictions.put(e.getKey(), result);
			
		}
		
		return predictions;
	}
	
}
