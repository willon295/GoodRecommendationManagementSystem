package cn.willon.bigdata.grms.s4_UserBuyGoodsVector;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
计算用户的购买向量
使用第一步输出作为输入
10001   20001,20005,20006,20007,20002
10002   20006,20003,20004
10003   20002,20007
10004   20001,20002,20005,20006
10005   20001
10006   20004,20007*/
public class UserBuyGoodsVector extends Configured implements Tool {

    public static class UserBuyGoodsVectorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        Text keyOut = new Text();
        IntWritable valueOut = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");
            Integer user = Integer.parseInt(split[0].trim());
            valueOut.set(user);
            String[] goods = split[1].split(",");
            for (String good : goods) {
                keyOut.set(good);
                context.write(keyOut, valueOut);
            }

        }
    }


    public static class UserBuyGoodsVectorReducer extends Reducer<Text, IntWritable, Text, Text> {


        Text valueOut = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            List<Integer>  l = new ArrayList<Integer>();
            for (IntWritable value : values) {
                l.add(value.get());
            }
            Collections.sort(l);
            for (Integer integer : l) {
                sb.append(integer).append(":1").append(",");

            }

            valueOut.set(sb.substring(0, sb.length() - 1));
            context.write(key, valueOut);
        }
    }


    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));
        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setMapperClass(UserBuyGoodsVectorMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, in);

        //设置reducer

        job.setReducerClass(UserBuyGoodsVectorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new UserBuyGoodsVector(), args));
    }
}

