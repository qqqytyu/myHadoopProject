package lll.hadoop.mr.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class threeJob {

    public static boolean runJob(Job job, Path in, Path out) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        /*
          分布式缓存
          Hadoop为MapReduce框架提供的一种分布式缓存机制
          它会将需要缓存的文件分发到各个执行任务的子节点的机器中
          各个节点可以自行读取本地文件系统上的数据进行处理
         */
        //job.addCacheFile(out.suffix("/one/part-r-00003").toUri());
//        job.addCacheFile(new URI(p03.toUri().toString()+"#003"));
        //job.addCacheFile(out.suffix("/two/part-r-00000").toUri());
//        job.addCacheFile(new URI(p00.toUri().toString()+"#000"));

//        System.out.println("\n\n\n\n++++++++++++++++++\n\n\n\n");
//        System.out.println(new URI(out.suffix("/one/part-r-00003").toString()+"#003").getPath());
//        System.out.println(new URI(out.suffix("/one/part-r-00003").toString()+"#003").toString());
//        System.out.println("\n\n\n\n++++++++++++++++++\n\n\n\n");

        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out.suffix("/three"));

        job.setMapperClass(threeMapper.class);
        job.setReducerClass(threeReducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        return job.waitForCompletion(true);

    }

}

class threeMapper extends Mapper<Text, Text, Text, Text>{

    //private static Logger logger = LoggerFactory.getLogger(threeMapper.class);

    private static int count = -1;

    private static Map<String, Integer> df = new HashMap<>();

    private static NumberFormat nf = NumberFormat.getInstance();

    private static Text outKey = new Text();

    private static Text outVal = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        if(count == -1 || df.size() == 0){

            nf.setMaximumFractionDigits(5);

            Path one = new Path("/lll/tfidf/output/one/part-r-00003");
            Path two = new Path("/lll/tfidf/output/two/part-r-00000");
            List<Path> li = new ArrayList<>();
            li.add(one);
            li.add(two);

//            for(URI uri : context.getCacheFiles()){
            for(Path p : li){

//                Path path = new Path(uri.toString());
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), "UTF-8"));
                System.out.println("system charsetName : \n *******************************>" + Charset.defaultCharset().name());
                String line = null;
                int num = 0;
                int innum = 0;

                while((line = br.readLine()) != null){
                    String[] ss = line.split("\t");
                    if(p.getName().contains("part-r-00000")){
                        if(df.containsKey(ss[0])) System.out.println("-------------------------------------------->!!!!" + ss[0]);
                        df.put(ss[0], Integer.valueOf(ss[1].trim()));
                        System.out.println("map size:\n" + df.size());
                        System.out.println("in:" + innum++);
                    } else if(p.getName().contains("part-r-00003")){
                        count = Integer.valueOf(ss[1].trim());
                    }
                    System.out.println(num++);
                }

                System.out.println("\n\n\n\n############################\n\n\n\n");
                System.out.println(df.size());
                System.out.println("\n\n\n\n############################\n\n\n\n");

                br.close();

            }

        }

    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        FileSplit fs = (FileSplit) context.getInputSplit();

        if (!fs.getPath().getName().contains("part-r-00003")) {

            String[] ss = key.toString().split("_");

            int tf = Integer.valueOf(value.toString());

            if(ss.length >= 2){

                String word = ss[1];

                System.out.println("\n\n\n\n+++++++++++++\n\n\n\n");
                System.out.println(df.get(word)+"");
                System.out.println(df.size());
                System.out.println(word);
                System.out.println(count+"");
                System.out.println(tf+"");
                System.out.println("\n\n\n\n+++++++++++++\n\n\n\n");

                double s = tf * Math.log((double) count / df.get(word));

                outKey.set(ss[0]);

                outVal.set(word + ":" + nf.format(s));

                context.write(outKey, outVal);

            }

        }

    }

}

class threeReducer extends Reducer<Text, Text, Text, Text>{

    private static Text outVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();

        for(Text t : values)
            sb.append(t.toString()).append("\t");

        outVal.set(sb.toString());

        context.write(key, outVal);

    }

}
