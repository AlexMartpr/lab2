package bmstu;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinAirportReducer extends Reducer<AirportWritableComparable, Text, Text, Text> {
    
    private final int positionAirportName = 14;
    
    @Override
    protected void reduce(AirportWritableComparable arg0, Iterable<Text> arg1,
            Reducer<AirportWritableComparable, Text, Text, Text>.Context arg2)
            throws IOException, InterruptedException {

        float minDelay = Float.MAX_VALUE;
        float maxDelay = 0f;
        float averageDelay = 0f;
        float summary = 0f;
        float count = 0f;
        Iterator<Text> iter = arg1.iterator();
        if (iter.hasNext()) {
            String airportName = iter.next().toString();
            while()
        }
    }

}
