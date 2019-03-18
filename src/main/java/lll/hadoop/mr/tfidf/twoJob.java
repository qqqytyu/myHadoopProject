package lll.hadoop.mr.tfidf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class twoJob {

    public static boolean runJob(Job job, Path in, Path out) throws IOException, ClassNotFoundException, InterruptedException {

        FileInputFormat.addInputPath(job, in);

        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(twoMapper.class);
        job.setReducerClass(twoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(twoReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        return job.waitForCompletion(true);

    }

}

class twoMapper extends Mapper<Text, Text, Text, IntWritable>{

    private static Text outKey = new Text();

    private static IntWritable outVal = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        FileSplit fs = (FileSplit)context.getInputSplit();

        if(!fs.getPath().getName().contains("part-r-00003")){
            outKey.set(key.toString().split("_")[1]);
            context.write(outKey, outVal);
        }

    }

}

class twoReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    private static IntWritable out = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for(IntWritable i : values){
            int n = i.get();
            sum += n;
        }

        out.set(sum);

        context.write(key, out);

    }

}
