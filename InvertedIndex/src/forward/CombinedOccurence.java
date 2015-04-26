package forward;

import java.util.ArrayList;

public class CombinedOccurence {
	public String url;
	public double tf;
	public ArrayList<Integer> positions;
	
	public CombinedOccurence(String url) {
		this.url = url;
		this.positions = new ArrayList<Integer>();
	}
	
	public void setTF(double tf) {
		this.tf = tf;
	}

	public void addPosition(int position) {
		positions.add(position);
	}
	
	public String toString() {
		return url + " " + tf + " " + positions;
	}

}
