package lll.hadoop.mr.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class myFof {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 2)
            throw new IOException("Usage: yarn jar lll_Hadoop_Test.jar lll.hadoop.mr.fof.myFof <input> <output>");


        Configuration conf = new Configuration(true);

        Job job = Job.getInstance(conf);

        job.setJarByClass(myFof.class);

        job.setJobName("my fof");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(myFofMapper.class);
        job.setReducerClass(myFofReducer.class);

        job.setMapOutputKeyClass(myFofKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setGroupingComparatorClass(myFofGrouping.class);

        job.waitForCompletion(true);

    }

}
