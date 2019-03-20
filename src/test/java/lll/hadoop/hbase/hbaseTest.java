package lll.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class hbaseTest {

    private Connection conn;

    private Admin admin;

//    Table table;

    @Before
    public void before() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","node002,node003,node004");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
//        table = conn.getTable(TableName.valueOf("lll_table"));
    }

    @After
    public void after() throws IOException {
        if(admin != null)
            admin.close();
        if(conn != null)
            conn.close();
    }

    /**
     * Hbase创建表
     */
    @Test
    public void createTale() throws IOException {

        //创建表的数据信息
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("lll:lll_table"));

        //创建列簇的数据信息
        HColumnDescriptor hcd = new HColumnDescriptor("cf01");

        //将列簇的信息添加到表中
        htd.addFamily(hcd);

        //修改列簇信息请使用下面的方法
        //htd.modifyFamily(hcd);

        //判断要创建的表是否存在
        if(admin.tableExists(TableName.valueOf("lll_table")))
            System.out.println("create table failure: table already exist ");

        //创建表
        admin.createTable(htd);

        System.out.println(htd.getTableName().getNameAsString() + "create a success!");

    }

    /**
     * Hbase删除表
     */
    @Test
    public void deleteTable() throws IOException {

        if(admin.tableExists(TableName.valueOf("lll:lll_table"))){

            admin.disableTable(TableName.valueOf("lll:lll_table"));

            //通知服务器将表禁用，但不等待禁用结果，直接返回（异步）
//            admin.disableTableAsync(TableName.valueOf("lll_table"));

            admin.deleteTable(TableName.valueOf("lll:lll_table"));

//            admin.deleteTables("lll_table");

            System.out.println("table delete success!");

        }

    }

    /**
     * Hbase插入数据
     */
    @Test
    public void insertData() throws IOException {

        //获取一个表的实例
        Table table = conn.getTable(TableName.valueOf("lll:lll_table"));

        List<Put> puts = new ArrayList<>();

        //创建一个Put操作实例，可对单个行进行操作，创建时需传入rowkey
        Put put01 = new Put("111111rowKey111111".getBytes());
        put01.addColumn("cf01".getBytes(), "name".getBytes(), "zhangsan".getBytes());
        put01.addColumn("cf01".getBytes(), "age".getBytes(), "18".getBytes());
        put01.addColumn("cf01".getBytes(), "sex".getBytes(), "woman".getBytes());
        puts.add(put01);

        Put put02 = new Put("222222rowKey222222".getBytes());
        put02.addColumn("cf01".getBytes(), "name".getBytes(), "lisi".getBytes());
        put02.addColumn("cf01".getBytes(), "age".getBytes(), "20".getBytes());
        put02.addColumn("cf01".getBytes(), "sex".getBytes(), "man".getBytes());
        puts.add(put02);

        //执行put操作，可执行一个或多个
        table.put(puts);

        System.out.println("data insert success!");
        table.close();
    }

    /**
     * Hbase get操作
     */
    @Test
    public void get() throws IOException {

        Table table = conn.getTable(TableName.valueOf("lll:lll_table"));

        //创建一个Get操作实例，要指定rowkey值
        Get get = new Get("111111rowKey111111".getBytes());

        //指定需要取出哪列数据，当所需大量条数据，但只需个别字段时，可以用这个方法在服务端过滤输出
        get.addColumn("cf01".getBytes(), "name".getBytes());
        get.addColumn("cf01".getBytes(), "sex".getBytes());

        //获取数据
        Result rs = table.get(get);

        //获取指定时间戳版本的数据
//        get.setTimeStamp(1L);

        //设置一次性能取出几个版本的数据
//        get.setMaxVersions(3);

        //获取指定的字段
        Cell cell01 = rs.getColumnLatestCell("cf01".getBytes(),"name".getBytes());
        Cell cell02 = rs.getColumnLatestCell("cf01".getBytes(),"sex".getBytes());

        System.out.println(Bytes.toString(CellUtil.cloneValue(cell01)));
        System.out.println(Bytes.toString(CellUtil.cloneValue(cell02)));

        table.close();

    }

    /**
     * Hbase scan操作
     */
    @Test
    public void scanTable() throws IOException {

        Table table = conn.getTable(TableName.valueOf("lll:lll_table"));

        //创建一个scan操作实例
        Scan scan = new Scan();

        //设置扫表的起始、结束rowkey，不设置的话就扫全表，包含起始不包含结束
        scan.setStartRow("111111rowKey111111".getBytes());
        scan.setStopRow("222222rowKey222222".getBytes());

        //scan也可以添加行过滤
//        scan.addColumn();

        ResultScanner rss = table.getScanner(scan);

        printRs(rss);

        rss.close();
        table.close();

    }

    /**
     * Hbase 过滤器
     */
    @Test
    public void filter() throws IOException {

        Table table = conn.getTable(TableName.valueOf("lll:lll_table"));
        Scan scan = new Scan();
        //创建一个过滤器组，MUST_PASS_ALL是所有条件都要满足，MUST_PASS_ONE是只要满足一个
        FilterList fls = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //创建一个过滤器
        SingleColumnValueFilter scf = new SingleColumnValueFilter("cf01".getBytes(), "age".getBytes(), CompareFilter.CompareOp.EQUAL, "18".getBytes());
        //将过滤器添加到过滤器组
        fls.addFilter(scf);
        //对scan设置过滤器
        scan.setFilter(fls);
        ResultScanner rss = table.getScanner(scan);
        printRs(rss);
        rss.close();
        table.close();

    }

    private void printRs(ResultScanner rss){
        for(Result rs : rss){
            Cell cell01 = rs.getColumnLatestCell("cf01".getBytes(),"name".getBytes());
            Cell cell02 = rs.getColumnLatestCell("cf01".getBytes(),"sex".getBytes());
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell01)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell02)));
        }
    }

}
