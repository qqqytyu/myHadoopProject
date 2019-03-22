package lll.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class hbaseMR {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration(true);

        conf.set("mapreduce.app-submission.cross-platform", "true");

        conf.set("mapreduce.job.jar", "E:\\IDEA_GitHub\\myHadoop\\target\\lll_hadoop_test.jar");

        conf.set("hbase.zookeeper.quorum","node002,node003,node004");

        conf.set("hbase.zookeeper.property.clientPort","2181");

        Job job = Job.getInstance(conf);

        job.setJarByClass(hbaseMR.class);

        job.setJobName("hbase mr");

        FileInputFormat.addInputPath(job, new Path("/user/lll/wc.txt"));

        job.setMapperClass(hbaseMRmapper.class);

        TableMapReduceUtil.initTableReducerJob("lll:wc", hbaseMRreduce.class, job, null, null, null, null, false);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        job.waitForCompletion(true);

    }

}

class hbaseMRmapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        for(String str : value.toString().split(" "))
            context.write(new Text(str), new IntWritable(1));

    }

}

class hbaseMRreduce extends TableReducer<Text, IntWritable,ImmutableBytesWritable>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int num = 100;

//        for(IntWritable iw : values)
//            num += iw.get();

        Put put = new Put(key.toString().getBytes());

        put.addColumn("cf01".getBytes(),"number".getBytes(),String.valueOf(num).getBytes());

        context.write(new ImmutableBytesWritable(key.toString().getBytes()),put);

    }
}
