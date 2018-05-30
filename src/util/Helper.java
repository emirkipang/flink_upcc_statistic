package util;

public class Helper {
	public static String isNull(String text) {
		return (text == null) ? "UNKNOWN" : text;
	}

	public static String combineFileds(int start, int end, String[] items,
			String delimiter) {
		String result = items[start];
		for (int i = start + 1; i <= end; i++) {
			result = result + delimiter + items[i];
		}

		return result;
	}

	public static String joinRule(String in, int length) {
		int gap = length - in.length();

		if (gap != 0) {
			for (int i = 1; i <= gap; i++) {
				in = "0" + in;
			}
		}

		return in;
	}
}
