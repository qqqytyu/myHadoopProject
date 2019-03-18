package lll.hadoop.mr.pageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class pageRankMapper extends Mapper<Text, Text, Text, Text> {

    private Text outKey = new Text();

    private Text outValue = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //data:<"A","B	D">

        int runNum = context.getConfiguration().getInt("runNum", 1);

        DataNode dn;

        if(runNum == 0)
            dn = DataNode.formaNode("1.0", value.toString());
        else
            dn = DataNode.formaNode(value.toString());

        String keyStr = key.toString();

        outValue.set(dn.toString());

        context.write(key, outValue);

        if(!dn.getAdjacentNodeNames().isEmpty() && dn.getAdjacentNodeNames().size() > 1){

            double weight = dn.getWeight() / dn.getAdjacentNodeNames().size();

            for(String node : dn.getAdjacentNodeNames()){

                outKey.set(node);

                outValue.set(String.valueOf(weight));

                context.write(outKey, outValue);

            }

        }

    }

}
