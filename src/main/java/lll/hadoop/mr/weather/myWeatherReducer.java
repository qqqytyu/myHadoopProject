package lll.hadoop.mr.weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class myWeatherReducer extends Reducer<myWeatherText, Text, Text, Text> {

    private Text tt = new Text();

    @Override
    protected void reduce(myWeatherText key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String firstDay = null;

        for(Text t : values){

            if(firstDay == null){

                firstDay = key.getDay();
                tt.set(String.valueOf(key.hashCode()));
                context.write(t, tt);

            } else {

                if(!firstDay.equals(key.getDay())){
                    tt.set(String.valueOf(key.hashCode()));
                    context.write(t, tt);

                    break;

                }

            }

        }

    }

}
