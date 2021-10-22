package bmstu;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class AirportWritableComparable implements WritableComparable<AirportWritableComparable>{

    private int airportID;
    private int dataID;

    public AirportWritableComparable() {};

    public AirportWritableComparable(int aID, int dID) {
        this.airportID = aID;
        this.dataID = dID;
    }

    public int getAirportID() {
        return this.airportID;
    }

    public int getDataID() {
        return this.dataID;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(airportID);
        out.writeInt(dataID);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        airportID = in.readInt();
        dataID = in.readInt();
        
    }

    @Override
    public int compareTo(AirportWritableComparable o) {
        int compareAirportID = airportID > o.airportID ? 1 : -1;
        if (airportID == o.airportID) compareAirportID = 0;
        if (compareAirportID != 0) {
            return compareAirportID;
        } else {
            return Integer.compare(dataID, o.dataID);
        }
    }
    
}
