package lll.hadoop.mr.weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class myWeatherPartitioner extends Partitioner<myWeatherText, Text> {

    @Override
    public int getPartition(myWeatherText wt, Text text, int numPartitions) {
        return Integer.valueOf(wt.getMonth()) % numPartitions;
    }

}
