package forward;

import java.util.ArrayList;

public class CombinedOccurence {
	public String url;
	public double tf;// term frequency
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
		String posList = "";
		for (int i = 0; i < positions.size(); i++) {
			posList += positions.get(i);
			if (i != positions.size() - 1)
				posList += ",";
				
		}
		
		
		
		return url + " " + String.format("%.3f", tf) + " " + posList;
	}

}
