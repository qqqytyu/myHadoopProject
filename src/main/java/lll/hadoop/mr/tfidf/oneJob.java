package lll.hadoop.mr.tfidf;

import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.StandardTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

public class oneJob {

    public static boolean runJob(Job job, Path in, Path out) throws IOException, ClassNotFoundException, InterruptedException {

        FileInputFormat.addInputPath(job, in);

        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(oneMapper.class);
        job.setReducerClass(oneReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(oneReducer.class);
        job.setPartitionerClass(onePartitionr.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setNumReduceTasks(4);

        return job.waitForCompletion(true);

    }

}

class oneMapper extends Mapper<Text, Text, Text, IntWritable>{

    private static IntWritable outVal = new IntWritable(1);

    private static Text outKey = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        String valStr = value.toString();

        String keyStr = key.toString();

        if(valStr != null && !valStr.trim().isEmpty()){

            List<Term> termList = StandardTokenizer.segment(valStr);

            for(Term t : termList){

                outKey.set(keyStr + "_" +t.word);

                context.write(outKey, outVal);

            }

            outKey.set("count");

            context.write(outKey, outVal);

        }

    }
}

class oneReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    private static IntWritable outVal = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int num = 0;

        for(IntWritable i : values){
            num += i.get();
        }

        outVal.set(num);

        context.write(key, outVal);

    }

}

class onePartitionr extends HashPartitioner<Text, IntWritable>{

    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {

        if(key.toString().equals("count")) return 3;

        return super.getPartition(key, value, numReduceTasks -1);

    }

}
