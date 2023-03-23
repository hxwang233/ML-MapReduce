package lab4;

import java.util.ArrayList;

public class Data {
	private ArrayList<Double> features;

	public Data() {
		features = new ArrayList<Double>();
	}
	
	public ArrayList<Double> getFeatures(){
		return this.features;
	}
	
	public void addFeature(Double f) {
		features.add(f);
	}

}
