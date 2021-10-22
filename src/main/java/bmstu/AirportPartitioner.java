package bmstu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AirportPartitioner extends Partitioner<AirportWritableComparable, Text>{

    @Override
    public int getPartition(AirportWritableComparable key, Text value, int numPartitions) {
        int hash = key.getAirportID() % numPartitions;
        return hash;
    }
        
}
