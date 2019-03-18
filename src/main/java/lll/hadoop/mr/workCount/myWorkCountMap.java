package lll.hadoop.mr.workCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class myWorkCountMap extends Mapper<LongWritable, Text, Text, LongWritable> {

    private LongWritable valOut = new LongWritable(1);
    private Text keyOut = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] strs = value.toString().split(" ");

        for (String s: strs) {
            keyOut.set(s);
            context.write(keyOut, valOut);
        }

    }
}
