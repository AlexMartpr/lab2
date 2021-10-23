package bmstu;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinAirportReducer extends Reducer<AirportWritableComparable, Text, Text, Text> {
    
    @Override
    protected void reduce(AirportWritableComparable arg0, Iterable<Text> arg1,
            Reducer<AirportWritableComparable, Text, Text, Text>.Context arg2)
            throws IOException, InterruptedException {

        Iterator<Text> iter = arg1.iterator();
        if (iter.hasNext()) {
            Text airportName = new Text(iter.next());
            float minDelay = Float.MAX_VALUE;
            float maxDelay = 0;
            float summary = 0;
            int count = 0;
            while(iter.hasNext()) {
                String delayString = iter.next().toString();
                float delay = Float.parseFloat(delayString);
                minDelay = minDelay > delay ? delay : minDelay;
                maxDelay = delay > maxDelay ? delay : maxDelay;
                summary += delay;
                count++;
            }
                if (count > 0) {
                    String avDelay = "\nAverage Delay: " + summary / count + " min\n";
                    String minimumDelay = "Min delay: " + minDelay + " min\n";
                    String maximumDelay = "Max delay: " + maxDelay + " min\n";
                    arg2.write(airportName, new Text(avDelay + minimumDelay + maximumDelay));
                }
            }
        }

}
