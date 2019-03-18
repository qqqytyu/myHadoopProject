package lll.hadoop.mr.pageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RunJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String jarPath = "D:\\IDEA_MAVEN_CODE\\myProject\\target\\lll_Hadoop_Test.jar";

        Path initial_input = new Path("/lll/pageRank/input");

        Path initial_output = new Path("/lll/pageRank/output");

        Configuration conf = new Configuration(true);

        // 跨平台提交作业
        conf.set("mapreduce.app-submission.corss-paltform", "true");

        // 切换分布式到本地单进程模拟运行
//        conf.set("mapreduce.framework.name", "local");

        //mapred做本地计算所使用的文件夹，可以配置多块硬盘，逗号分隔
//        conf.set("mapreduce.cluster.local.dir", "D:\\mapreduce.cluster.local.dir");

        //提交jar包到yarn上运行
        conf.set("mapreduce.job.jar", jarPath);

        double accuracy = 0.001;

        int num = 0;

        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(initial_output)) fs.delete(initial_output,true);

        fs.close();

        while (true){

            conf.setInt("runNum", num);

            Job job = Job.getInstance(conf);

            job.setJarByClass(RunJob.class);

            job.setJobName("page rank");

            if(num == 0)
                FileInputFormat.addInputPath(job, initial_input);
            else
                FileInputFormat.addInputPath(job, initial_output.suffix(String.format("/%d",num -1)));

            FileOutputFormat.setOutputPath(job, initial_output.suffix(String.format("/%d",num++)));

            job.setMapperClass(pageRankMapper.class);
            job.setReducerClass(pageRankReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setInputFormatClass(KeyValueTextInputFormat.class);

            if(job.waitForCompletion(true)){

                long sum = job.getCounters()
                        .findCounter("lll.hadoop","pageRank").getValue();

                if(sum / 4000.0 < accuracy)
                    break;

            }

        }

    }

}
