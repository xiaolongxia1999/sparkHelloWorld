package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;

import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


import java.io.IOException;

/**
 * Created by Administrator on 2018/5/1 0001.
 */

//使用hbase的服务，需要添加依赖包：hbase-server，而不是hbase-common或hbase-client
public class hdfsToHbase {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputPath = "hdfs://172.16.32.139:9000/usr/cl/test.txt";

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","172.16.32.139:2181,172.16.32.142:2181,172.16.32.132:2181,172.16.32.143:2181,172.16.32.121:2181");
        conf.set(TableOutputFormat.OUTPUT_TABLE,"GaoAnTun");
//        conf.set("dfs.socket.timeout","180000");

        Job job = new Job(conf, "HBaseBatchImport");
        job.setMapperClass(BatchImportMapper.class);
        job.setReducerClass(BatchImportReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);


        FileInputFormat.setInputPaths(job,inputPath);

        job.waitForCompletion(true);
    }

    public static class BatchImportMapper extends Mapper<LongWritable,Text,LongWritable,Text>{
        Text v2 = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("map!");
            try {
                String[] split = value.toString().split(",");
                String rowKey = split[0]+split[4];
                v2.set(rowKey+","+value.toString());
                System.out.println("map___");
                System.out.println("rowKey:"+rowKey);
                context.write(key,v2);

            }catch (NumberFormatException e){
                final Counter counter = context.getCounter("BatchImport","ErrorFormat");
                counter.increment(1L);
                System.out.println("出错了:"+e.getMessage());

            }
        }
    }

    public static class BatchImportReducer extends TableReducer<LongWritable,Text,NullWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("reduce!");
            for(Text text:values){
                String[] split = text.toString().split(",");
                Put put = new Put(Bytes.toBytes(split[0]));
                put.add(Bytes.toBytes("cf1"),Bytes.toBytes("id"),Bytes.toBytes(split[1]));
                put.add(Bytes.toBytes("cf1"),Bytes.toBytes("unit"),Bytes.toBytes(split[2]));
                put.add(Bytes.toBytes("cf1"),Bytes.toBytes("englishName"),Bytes.toBytes(split[3]));
                put.add(Bytes.toBytes("cf1"),Bytes.toBytes("value"),Bytes.toBytes(split[4]));
                put.add(Bytes.toBytes("cf1"),Bytes.toBytes("time"),Bytes.toBytes(split[5]));

                context.write(NullWritable.get(),put);
            }
        }
    }
}
