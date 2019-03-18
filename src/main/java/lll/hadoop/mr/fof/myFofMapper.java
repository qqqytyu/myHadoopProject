package lll.hadoop.mr.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class myFofMapper extends Mapper<LongWritable, Text, myFofKey, IntWritable> {

    private myFofKey fofKey = new myFofKey();

    private IntWritable intVal = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] ds = value.toString().split(" ");

        for(int i = 0; i < ds.length - 1; i++){

            for(int y = i + 1; y < ds.length; y++){

                if(i == 0) {

                    intVal.set(0);

                    context.write(fofKeyConf(ds[i], ds[y], fofKey, 0), intVal);

                }else{

                    intVal.set(1);

                    context.write(fofKeyConf(ds[i], ds[y], fofKey, 1), intVal);

                }

            }

        }

    }

    private myFofKey fofKeyConf(String str1, String str2, myFofKey fof, int weight){

        if(str1.compareTo(str2) > 0)

            fof.set(str2, str1, weight);

        else

            fof.set(str1, str2, weight);

        return fof;

    }

}
