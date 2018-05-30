package main;

import java.util.HashMap;
import java.util.Map;

import join.UpccLeftJoinLacima;
import map.StatisticFlatMap;
import model.Upcc;

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType;

import util.Constant;

public class Main {
	private HashMap<String, DataSet<String>> dataset_inputs = new HashMap<String, DataSet<String>>();
	private ExecutionEnvironment env;
	private int proses_paralel;
	private int sink_paralel;
	private Configuration parameter;
	private String outputPath;
	private String period;

	// tuples variable
	private DataSet<Tuple3<Integer, Long, Long>> upcc_tuples;

	public Main(int proses_paralel, int sink_paralel, String outputPath) {
		this.env = ExecutionEnvironment.getExecutionEnvironment();
		this.parameter = new Configuration();
		this.outputPath = outputPath;

		this.proses_paralel = proses_paralel;
		this.sink_paralel = sink_paralel;
		this.env.setParallelism(this.proses_paralel);
		this.parameter.setBoolean("recursive.file.enumeration", true);

		// BasicConfigurator.configure(); //remove log warn

	}

	private String getOutputPath() {
		return this.outputPath;
	}

	private Configuration getParameter() {
		return this.parameter;
	}

	private ExecutionEnvironment getEnv() {
		return this.env;
	}

	public int getSink_paralel() {
		return this.sink_paralel;
	}

	private void setInput(HashMap<String, String> files) {

		for (Map.Entry<String, String> file : files.entrySet()) {
			dataset_inputs.put(
					file.getKey(),
					getEnv().readTextFile(file.getValue()).withParameters(
							getParameter()));
		}

	}

	public void processInput() {
		upcc_tuples = dataset_inputs.get("source_upcc").flatMap(
				new StatisticFlatMap());

	}

	public void processAggregate() {
		// 1. upcc summary
		upcc_tuples = upcc_tuples.groupBy(0).aggregate(Aggregations.SUM, 1)
				.and(Aggregations.SUM, 2);

	}

	public void sink() throws Exception {
		upcc_tuples.writeAsCsv(getOutputPath(), "\n", "|", WriteMode.OVERWRITE)
				.setParallelism(getSink_paralel());

	}

	public static void main(String[] args) throws Exception {
		// set data input
		HashMap<String, String> files = new HashMap<String, String>();

		/** prod **/
		ParameterTool params = ParameterTool.fromArgs(args);

		int proses_paralel = params.getInt("slot");
		int sink_paralel = params.getInt("sink");
		String source = params.get("source");
		String output = params.get("output");

		Main main = new Main(proses_paralel, sink_paralel, output);

		files.put("source_upcc", source);

		/** dev **/
		// int proses_paralel = 2;
		// int sink_paralel = 1;
		// String period = "1";
		//
		// Main main = new Main(proses_paralel, sink_paralel, Constant.OUTPUT,
		// period);
		// files.put("source_upcc", Constant.FILE_UPCC);
		// files.put("source_lacima_3g", Constant.FILE_LACIMA_3G);
		// files.put("source_lacima_4g", Constant.FILE_LACIMA_4G);

		/****/
		main.setInput(files);
		main.processInput();
		main.processAggregate();
		main.sink();

		try {
			main.getEnv().execute("job flink upcc");
		} catch (Exception e) {
			// TODO Auto-generated catch blockF
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}
}
