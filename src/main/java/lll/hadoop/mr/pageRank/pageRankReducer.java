package lll.hadoop.mr.pageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class pageRankReducer extends Reducer<Text, Text, Text, Text> {

    private Text outVal = new Text();

    private Text outKey = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        double sum = 0;

        String ans = "";

        double oldSum = 0.00;

        for(Text t : values){

            String[] ss = t.toString().split(":");

            if(ss.length > 1){
                ans = String.format(":%s", ss[1]);
                oldSum = Double.valueOf(ss[0]);
            }
            else
                sum += Double.valueOf(ss[0]);

        }

        double newSum = (0.15 / 4.0) + (0.85 * sum);

        outVal.set(String.format("%s%s",String.valueOf(newSum), ans));

        context.write(key, outVal);

        context.getCounter("lll.hadoop","pageRank").increment(
                Math.abs((int)((newSum - oldSum)* 1000))
        );

    }

}
