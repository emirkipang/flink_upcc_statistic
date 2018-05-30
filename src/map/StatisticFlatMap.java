package map;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import model.Upcc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import util.Constant;
import util.Helper;

public class StatisticFlatMap implements
		FlatMapFunction<String, Tuple3<Integer, Long, Long>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2441530947556276735L;

	/**
* 
*/

	@Override
	public void flatMap(String in,
			Collector<Tuple3<Integer, Long, Long>> out) throws Exception {
		// TODO Auto-generated method stub
		String[] lines = in.split("\n");

		for (String line : lines) {

			String[] items = line.split(",", -1);
			
			int triggerType = Integer.parseInt(items[0]);
			
			long BonusUsage = items[31].equals("") ? 0 : Long.parseLong(items[31]);
			long triggerType0 = BonusUsage == 0 ? 1 : 0;
			long triggerTypeN = BonusUsage > 0 ? 1 : 0;

			
			out.collect(new Tuple3<Integer, Long, Long>(triggerType, triggerType0, triggerTypeN));

		}

	}

}
