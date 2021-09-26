import java.awt.BasicStroke;
import java.util.ArrayList;

import javax.swing.JFrame;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;



public class ChartUI2 extends JFrame {

    private static final long serialVersionUID = 1L;

    public ChartUI2(String applicationTitle, String chartTitle, ArrayList<XYSeries> series) {
        super(applicationTitle);
        // This will create the dataset
        XYDataset dataset = createDataset(series);
        // based on the dataset we create the chart
        JFreeChart chart = createChart(dataset, chartTitle);
        // we put the chart into a panel
        ChartPanel chartPanel = new ChartPanel(chart);
        // default size
        chartPanel.setPreferredSize(new java.awt.Dimension(1000, 500));
        // add it to our application
        setContentPane(chartPanel);

    }

    /**
     * Creates a sample dataset
     */
    private  XYDataset createDataset(ArrayList<XYSeries> series) {
    	
        XYSeriesCollection dataset = new XYSeriesCollection();
        for(int i=0;i < series.size(); i++) {
        	dataset.addSeries(series.get(i));
        }
        return dataset;

    }

    /**
     * Creates a chart
     */
    private JFreeChart createChart(XYDataset dataset, String title) {

        JFreeChart chart = ChartFactory.createXYLineChart(
            title,                  // chart title
            "",                // data
            "",                   // include legend
            dataset
        );

        for(int i =0; i< dataset.getSeriesCount();i++) {
        	chart.getXYPlot().getRenderer().setSeriesStroke(i,new BasicStroke(2.0f));
        }
        
        return chart;

    }
}