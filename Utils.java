

import java.util.ArrayList;

public class Utils {
	
	public double dis(Data d1, Data d2) {
		ArrayList<Integer> f1 = d1.getFeatures();
		ArrayList<Integer> f2 = d2.getFeatures();
		int temp = 0;
		for (int i = 0; i < f1.size(); i++) {
			temp += Math.pow(f1.get(i) - f2.get(i), 2);
		}
		return Math.sqrt(temp);
	}
	
	public int getMaxIndex(Double[] karray) {
		int maxIndex = 0;
		for (int i = 1; i < karray.length; i++) {
			if (karray[i] > karray[maxIndex]) {
				maxIndex = i;
			}
		}
		return maxIndex;
	}
}
