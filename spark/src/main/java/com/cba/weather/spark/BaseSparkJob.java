package com.cba.weather.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import com.cba.weather.model.Observation;
import com.cba.weather.model.Station;

public abstract class BaseSparkJob {
	
	private SparkConf conf;
	
	@SuppressWarnings({ "serial", "rawtypes" })
	public static final List<Class> KRYO_CLASSES = new ArrayList<Class>() {{
        add(Observation.class);
        add(Station.class);
    }};

    public BaseSparkJob() {
		int numOfThreads = Runtime.getRuntime().availableProcessors();
		numOfThreads = numOfThreads > 2 ? (numOfThreads / 2) : 1;
		conf = new SparkConf().setAppName(getClass().getName()).setMaster("local[" + numOfThreads + "]");
		conf.registerKryoClasses(KRYO_CLASSES.toArray(new Class[KRYO_CLASSES.size()]));
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
	}
	
	protected SparkSession session() {
		return SparkSession.builder().config(conf).getOrCreate();
	}
	
	protected SQLContext sqlContext() {
		return session().sqlContext();
	}
	
	protected JavaSparkContext javaContext() {
		return new JavaSparkContext(session().sparkContext());
	}
	
	protected abstract void start() throws InterruptedException;
	
}
