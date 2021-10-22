package bmstu;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinAirportReducer extends Reducer<AirportWritableComparable, Text, Text, Text> {
    
    @Override
    protected void reduce(AirportWritableComparable arg0, Iterable<Text> arg1,
            Context arg2)
            throws IOException, InterruptedException {

        float minDelay = Float.MAX_VALUE;
        float maxDelay = 0f;
        float summary = 0f;
        float count = 0f;
        Iterator<Text> iter = arg1.iterator();
        if (iter.hasNext()) {
            Text airportName = iter.next();
            while(iter.hasNext()) {
                String delayString = iter.next().toString();
                float delay = Float.parseFloat(delayString);
                minDelay = minDelay > delay ? delay : minDelay;
                maxDelay = delay > maxDelay ? delay : maxDelay;
                summary += delay;
                count++;
                float averageDelay = summary / count;
                if (count > 0) {
                    String avDelay = "\nAverage Delay: " + averageDelay + " min\n";
                    String minimumDelay = "Min delay: " + minDelay + " min\n";
                    String maximumDelay = "Max delay: " + maxDelay + " min\n";
                    arg2.write(new Text(airportName), new Text(avDelay + minimumDelay + maximumDelay));
                }
            }
        }
    }

}
