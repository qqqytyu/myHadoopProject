package lll.hadoop.mr.weather;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class myWeatherMapper extends Mapper<LongWritable, Text, myWeatherText, Text> {

    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    private Calendar cal = Calendar.getInstance();

    private myWeatherText wt = new myWeatherText();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] vals = line.split("\t");

        try {
            cal.setTime(format.parse(vals[0]));
        } catch (ParseException e) {
            throw new IOException(e);
        }

        wt.set(
                String.valueOf(cal.get(Calendar.YEAR)),
                String.valueOf(cal.get(Calendar.MONTH)),
                String.valueOf(cal.get(Calendar.DATE)),
                Integer.valueOf(vals[1].substring(0,vals[1].length()-1))
        );

        context.write(wt, value);

    }

}
