package lll.hadoop.mr.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.net.URISyntaxException;

public class runJob {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        //定义输入路径
        Path tfidf_input = new Path("/lll/tfidf/input");

        //定义输出路径
        Path tfidf_output = new Path("/lll/tfidf/output");

        String jarPath = "D:\\IDEA_MAVEN_CODE\\myProject\\target\\lll_hadoop_test.jar";

        //获取hadoop配置
        Configuration conf = new Configuration(true);

        // 跨平台提交作业
        conf.set("mapreduce.app-submission.cross-platform", "true");

//        conf.set("mapreduce.framework.name", "local");

//        conf.set("mapreduce.cluster.local.dir", "D:\\tmp");

        //提交jar包到yarn上运行
        conf.set("mapreduce.job.jar", jarPath);

        String[] functions = new String[]{"one", "two", "three"};

//        FileSystem fs = FileSystem.get(conf);
//
//        if(fs.exists(tfidf_output)) fs.delete(tfidf_output,true);
//
//        fs.close();

        for(String functionName : functions){

            boolean isOk = false;

            Job job = Job.getInstance(conf);

            job.setJarByClass(runJob.class);

            job.setJobName(functionName + " run");

            switch (functionName){

                case "one": isOk = oneJob.runJob(job, tfidf_input, tfidf_output.suffix("/one"));break;

                case "two": isOk = twoJob.runJob(job, tfidf_output.suffix("/one"), tfidf_output.suffix("/two"));break;

                case "three":isOk = threeJob.runJob(job, tfidf_output.suffix("/one"), tfidf_output);break;

                default:break;

            }

            if(isOk) System.out.println(functionName + " is ok!");
            else System.out.println(functionName + " error");

        }

    }

}
