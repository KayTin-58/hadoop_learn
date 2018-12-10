package org.apress.prohadoop.c6;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


/**
 * 自定义键  需要继承 WritableComparable接口
 */
public class ArrivalFlightKey implements WritableComparable<ArrivalFlightKey> {
    
    public Text destinationAirport = new Text("");
    public Text arrivalDtTime = new Text("");

    public ArrivalFlightKey() {
    }

    public ArrivalFlightKey(Text destinationAirport,Text arrivalDtTime) {
        this.destinationAirport = destinationAirport;
        this.arrivalDtTime = arrivalDtTime;
    }

    @Override
    public int hashCode() {
        return (this.destinationAirport).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ArrivalFlightKey))
            return false;
        ArrivalFlightKey other = (ArrivalFlightKey) o;
        return this.destinationAirport.equals(other.destinationAirport) ;
    }

    
    public int compareTo(ArrivalFlightKey second) {
        return this.destinationAirport.compareTo(second.destinationAirport);
    }




    /**
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {

        this.destinationAirport.write(out);
        this.arrivalDtTime.write(out);
    }

    /**
     * 读数据的属性必须和写数据的属性一致
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        this.destinationAirport.readFields(in);
        this.arrivalDtTime.readFields(in);
    }
}