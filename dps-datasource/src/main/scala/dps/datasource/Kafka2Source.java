package dps.datasource;

import org.apache.spark.SparkContext;

import dps.datasource.define.DatasourceDefine;
import scala.collection.mutable.Map;

public class Kafka2Source extends DataSource {

	
	public Kafka2Source(SparkContext sparkContext, Map<String, String> params) {
		super(sparkContext, params);
		// TODO
	}
	
	@Override
	public Object read() {
		// TODO
		return null;
	}
	
	public DatasourceDefine define() {
		// TODO
		return null;
	}
}
