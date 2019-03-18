package lll.hadoop.mr.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class myFofReducer extends Reducer<myFofKey, IntWritable, Text, IntWritable> {

    private Text outKey = new Text();

    private IntWritable outVal = new IntWritable();

    @Override
    protected void reduce(myFofKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int num = 0;

        for(IntWritable i : values){

            if(i.get() == 0) return;

            num += i.get();

        }

        outKey.set(String.format("%s-%s",key.getProtagonist(),key.getSupport()));

        outVal.set(num);

        context.write(outKey, outVal);

//        for(IntWritable i : values){
//
//            outKey.set(String.format("%s-%s",key.getProtagonist(),key.getSupport()));
//
//            outVal.set(i.get());
//
//            context.write(outKey, outVal);
//
//        }
//        context.write(new Text("================="), new IntWritable(99));
    }

}
