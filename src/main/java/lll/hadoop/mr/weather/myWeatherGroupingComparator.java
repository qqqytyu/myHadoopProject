package lll.hadoop.mr.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class myWeatherGroupingComparator extends WritableComparator {

    public myWeatherGroupingComparator() {
        super(myWeatherText.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        myWeatherText wa = (myWeatherText)a;

        myWeatherText wb = (myWeatherText)b;

        int result = wa.getYear().compareTo(wb.getYear());

        if(result == 0){

            result = wa.getMonth().compareTo(wb.getMonth());

        }

        return result;

    }

}
