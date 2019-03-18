package lll.hadoop.mr.fof;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class myFofGrouping extends WritableComparator {

    public myFofGrouping() {

        super(myFofKey.class, true);

    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        myFofKey fa = (myFofKey) a;
        myFofKey fb = (myFofKey) b;

        return (fa.getProtagonist() + fa.getSupport()).compareTo(
                fb.getProtagonist() + fb.getSupport()
        );

    }

}
