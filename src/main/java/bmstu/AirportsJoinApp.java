package bmstu;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class AirportsJoinApp {
    public static void main(String[] args) throws IOException {
        Job job = Job.getInstance();
        job.setJarByClass(AirportsJoinApp.class);
        job.setJobName("JoinAirports");
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FlightMap.class);
        
    }
}
