package bmstu;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AirportComparator extends WritableComparator {
    public AirportComparator() {
        super(AirportWritableComparable.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        int firstAirportID = ((AirportWritableComparable)a).getAirportID();
        int secondAirportID = ((AirportWritableComparable)b).getAirportID();
        return Integer.compare(firstAirportID, secondAirportID);
    }
}
