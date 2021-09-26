import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.BiConsumer;

import org.jfree.data.xy.XYSeries;

public class ChartMaker {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		BufferedReader in;
		try {
			in = new BufferedReader(new FileReader("result/result.csv"));
			String currentLine;
			
			HashMap<String,XYSeries> seriesMap = new HashMap<String,XYSeries>();
			HashMap<String,HashMap<Double,Double>> distributions = new HashMap<String,HashMap<Double,Double>>();
		
			currentLine=in.readLine();
			
			ArrayList<String> hs = new ArrayList<String>();
			Double SpeedStep = 200000000d/10;
			
			
			while (currentLine!= null)
			{
				String[] vector = currentLine.split("\\|");
				if(!seriesMap.containsKey(vector[0]+" speed")) {
					hs.add(vector[0]);
					seriesMap.put(vector[0]+" speed", new XYSeries(vector[0]+" speed"));
					seriesMap.put(vector[0]+" error", new XYSeries(vector[0]+" error"));
					distributions.put(vector[0], new HashMap<Double,Double>());
				}
				seriesMap.get(vector[0]+" speed").add(Integer.parseInt(vector[1]),Double.parseDouble(vector[2])/1000000d);
				seriesMap.get(vector[0]+" error").add(Integer.parseInt(vector[1]),Double.parseDouble(vector[3]));
				if(distributions.get(vector[0]).containsKey((double)Math.round(Double.parseDouble(vector[2])/SpeedStep))) {
					distributions.get(vector[0]).put((double)Math.round(Double.parseDouble(vector[2])/SpeedStep),distributions.get(vector[0]).get((double)Math.round(Double.parseDouble(vector[2])/SpeedStep))+Double.parseDouble(vector[3]));
				}else {
					distributions.get(vector[0]).put((double)Math.round(Double.parseDouble(vector[2])/SpeedStep),Double.parseDouble(vector[3]));
				}
				
				currentLine=in.readLine();
			}
			
			ArrayList<XYSeries> seriesDistr = new ArrayList<XYSeries>();
			
			for(int i=0; i< hs.size(); i++) {
				ArrayList<XYSeries> series = new ArrayList<XYSeries>();
				series.add(seriesMap.get(hs.get(i)+" speed"));
				series.add(seriesMap.get(hs.get(i)+" error"));
				ChartUI2 demo = new ChartUI2("Internet speed and errors h="+hs.get(i), "Internet speed and errors h="+hs.get(i),series);
		        demo.pack();
		        demo.setVisible(true);
		        
		        XYSeries curser = new XYSeries(hs.get(i));
		       
				for(Double keySpeed : distributions.get(hs.get(i)).keySet().toArray(new Double[0])){
					curser.add(keySpeed*SpeedStep,distributions.get(hs.get(i)).get(keySpeed));
					curser.add(keySpeed*SpeedStep+SpeedStep-(SpeedStep/30)*(i+2),distributions.get(hs.get(i)).get(keySpeed));
		        }
				seriesDistr.add(curser);
			}
			
			ChartUI2 demo = new ChartUI2("Distribution of errors", "Distribution of errors",seriesDistr);
	        demo.pack();
	        demo.setVisible(true);
	        
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
