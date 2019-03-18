package lll.hadoop.mr.workCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class myWorkCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private LongWritable valOut = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long sum = 0;

        for(LongWritable lw : values)
            sum += lw.get();

        valOut.set(sum);

        context.write(key, valOut);

    }
}
