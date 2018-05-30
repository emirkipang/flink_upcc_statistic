package join;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

public class UpccLeftJoinLacima
		implements
		FlatJoinFunction<Tuple4<Long, String, String, String>, Tuple2<String, String>, Tuple1<String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void join(Tuple4<Long, String, String, String> leftElem,
			Tuple2<String, String> rightElem, Collector<Tuple1<String>> out)
			throws Exception {
		// TODO Auto-generated method stub
		String output = "";

		if (rightElem == null)
			output = leftElem.f1 + "|" + leftElem.f0 + "|" + leftElem.f3 + "|"
					+ "UNKNOWN" + "|" + "UNKNOWN" + "|" + "UNKNOWN" + "|"
					+ "UNKNOWN" + "|" + "UNKNOWN" + "|" + "UNKNOWN";

		else {
			output = leftElem.f1 + "|" + leftElem.f0 + "|" + leftElem.f3 + "|"
					+ rightElem.f1;

		}
		out.collect(new Tuple1<String>(output));

	}

}
