package bmstu;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirportMap extends Mapper<LongWritable, Text, Text, AirportWritableComparable>{
    @Override
    protected void map(LongWritable key, Text value,
            Mapper<LongWritable, Text, Text, AirportWritableComparable>.Context context)
            throws IOException, InterruptedException {
        String[] airport = value.toString().split("\",\"");
        if(key.get()!=0) {
            String clearStringAirportID = airport[0].replaceAll("\"", "");
            int airportID = Integer.parseInt(clearStringAirportID);
            String airportName = airport[1].replaceAll("\"", "");
        }
    }

}
