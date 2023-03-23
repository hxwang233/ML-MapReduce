package lab4;

import java.util.ArrayList;


public class Utils {
	
	public double dis(Data d1, Data d2) {
		//求距离
		ArrayList<Double> f1 = d1.getFeatures();
		ArrayList<Double> f2 = d2.getFeatures();
		int temp = 0;
		for (int i = 0; i < f1.size(); i++) {
			temp += Math.pow(f1.get(i) - f2.get(i), 2);
		}
		return Math.sqrt(temp);
	}
	
	public int getMinIndex(Double[] karray) {
		//获取距离最小索引
		int minIndex = 0;
		for (int i = 1; i < karray.length; i++) {
			if (karray[i] < karray[minIndex]) {
				minIndex = i;
			}
		}
		return minIndex;
	}
	
	public Data getMeans(ArrayList<Data> datum) {
		//按列求均值
		Data center = new Data();
		int fsize = datum.get(0).getFeatures().size();
		int count = 0;
		for (int i = 0; i < fsize; i++) {
			double temp = 0;
			for (Data data : datum) {
				if (i==0) {
					count += 1;
				}
				temp += data.getFeatures().get(i);
			}
			double mean = temp / count;
			center.addFeature(mean);
		}
		return center;
	}

	public boolean compareCenters(ArrayList<Data> centers, ArrayList<Data> newCenters) {
		//比较中心是否变化
		for (int i = 0; i < centers.size(); i++) {
			Data c1 = centers.get(i);
			Data c2 =  newCenters.get(i);
			for (int j = 0; j < c1.getFeatures().size(); j++) {
				if (Math.abs(c1.getFeatures().get(j)-c2.getFeatures().get(j)) > 1e-8) {
					return false;
				}
			}
		}
		return true;
	}
}
