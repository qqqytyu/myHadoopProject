package lll.hadoop.mr.pageRank;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DataNode {

    private static final String SEPARATOR = "\t";

    private double weight;

    private List<String> adjacentNodeNames;

    private DataNode(double weight, List<String> adjacentNodeNames) {
        this.weight = weight;
        this.adjacentNodeNames = adjacentNodeNames;
    }

    public static DataNode formaNode(String wt, String ans) throws IOException {

        return formaNode(String.format("%s%s%s",wt,SEPARATOR,ans));

    }

    public static DataNode formaNode(String nodeStr) throws IOException {

        String[] nodeArr = nodeStr.split("[:\t]");

        if(nodeArr.length >= 1){

            List<String> ans = null;

            if(nodeArr.length > 1)
                ans = Arrays.asList(Arrays.copyOfRange(nodeArr,1, nodeArr.length));

            return new DataNode(Double.valueOf(nodeArr[0]), ans);

        }else throw new IOException("nodeArr len not less than 1");

    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public List<String> getAdjacentNodeNames() {
        return adjacentNodeNames;
    }

    public void setAdjacentNodeNames(List<String> adjacentNodeNames) {
        this.adjacentNodeNames = adjacentNodeNames;
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();

        sb.append(this.weight);

        if(this.adjacentNodeNames != null)
            sb.append(":").append(StringUtils.join(this.adjacentNodeNames,SEPARATOR));

        return sb.toString();

    }
}
