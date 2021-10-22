package bmstu;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMap extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {
    
    private final int positionAirportID = 14;
    private final int positionAirportDelay = 18;

    @Override
    protected void map(LongWritable key, Text value,
            Mapper<LongWritable, Text, AirportWritableComparable, Text>.Context context)
            throws IOException, InterruptedException {
        String[] cleatValues = value.toString().split(",");
        if (key.get() > 0) {
            String airportStringID = cleatValues[positionAirportID];
            int airportID = Integer.parseInt(airportStringID);
            String airportDelay = cleatValues[positionAirportDelay];
            boolean checkDelay = Float.parseFloat(airportDelay) > 0.f;
            if (checkDelay && !airportDelay.isEmpty()) {
                context.write(new AirportWritableComparable(airportID, 1), new Text(airportDelay));
            }
        }
    }
}
