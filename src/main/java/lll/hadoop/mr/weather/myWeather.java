package lll.hadoop.mr.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class myWeather {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 2)
            throw new IOException("Usage: yarn jar lll_Hadoop_Test.jar lll.hadoop.mr.weather.myWeather <input> <output>");

        Configuration conf = new Configuration(true);

        Job job = Job.getInstance(conf);

        job.setJarByClass(myWeather.class);

        job.setJobName("my weather");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(myWeatherMapper.class);
        job.setReducerClass(myWeatherReducer.class);

        job.setMapOutputKeyClass(myWeatherText.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(myWeatherPartitioner.class);

        job.setGroupingComparatorClass(myWeatherGroupingComparator.class);

        job.setNumReduceTasks(3);

        job.waitForCompletion(true);

    }

}
