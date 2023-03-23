

import java.util.ArrayList;

public class Data {
	private ArrayList<Integer> features;
	private int label;
	
	public Data() {
		features = new ArrayList<Integer>();
	}
	
	public ArrayList<Integer> getFeatures(){
		return this.features;
	}
	
	public void addFeature(int f) {
		features.add(f);
	}

	public void setLabel(int label) {
		this.label = label;
	}
	
	public int getLabel() {
		return this.label;
	}
}
