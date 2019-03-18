package lll.hadoop.mr.workCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class myWorkCount {
    public static void main(String[] args) throws Exception {

        if (args.length != 2)
            throw new Exception("Usage: yarn jar wordcount.jar com.bjsxt.mr.wordcount.MainClass <input> <output>");

        Configuration conf = new Configuration(true);

        Job job = Job.getInstance(conf);

        job.setJarByClass(myWorkCount.class);

        job.setJobName("myJob");

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(myWorkCountMap.class);
        job.setReducerClass(myWorkCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.waitForCompletion(true);

    }
}
