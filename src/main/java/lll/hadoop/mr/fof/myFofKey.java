package lll.hadoop.mr.fof;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class myFofKey implements WritableComparable<myFofKey> {

    private String protagonist;

    private String support;

    private Integer weight;

    public myFofKey set(String protagonist, String support, Integer weight) {
        this.protagonist = protagonist;
        this.support = support;
        this.weight = weight;
        return this;
    }

    @Override
    public int compareTo(myFofKey o) {

        int result = (this.protagonist + this.support).compareTo(o.getProtagonist()  + o.getSupport());

        if(result == 0)

            result = this.weight.compareTo(o.getWeight());

        return result;

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.protagonist);
        out.writeUTF(this.support);
        out.writeInt(this.weight);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.setProtagonist(in.readUTF());
        this.setSupport(in.readUTF());
        this.setWeight(in.readInt());
    }

    public String getProtagonist() {
        return protagonist;
    }

    public void setProtagonist(String protagonist) {
        this.protagonist = protagonist;
    }

    public String getSupport() {
        return support;
    }

    public void setSupport(String support) {
        this.support = support;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }
}
