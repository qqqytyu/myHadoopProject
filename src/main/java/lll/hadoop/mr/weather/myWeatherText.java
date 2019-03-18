package lll.hadoop.mr.weather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class myWeatherText implements WritableComparable<myWeatherText> {

    private String year;

    private String month;

    private String day;

    private Integer temper;

    public myWeatherText() {}

    public void set(String year, String month, String day, Integer temper) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.temper = temper;
    }

    @Override
    public int compareTo(myWeatherText wt) {

        int result = this.year.compareTo(wt.getYear());

        if(result == 0){

            result = this.month.compareTo(wt.getMonth());

            if(result == 0){

                result = this.temper.compareTo(wt.getTemper());

            }

        }

        return result;

    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(this.year);

        out.writeUTF(this.month);

        out.writeUTF(this.day);

        out.writeInt(this.temper);

    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.setYear(in.readUTF());

        this.setMonth(in.readUTF());

        this.setDay(in.readUTF());

        this.setTemper(in.readInt());

    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public Integer getTemper() {
        return temper;
    }

    public void setTemper(Integer temper) {
        this.temper = temper;
    }
}
