package cn.willon.bigdata.grms.s2_GoodsCooCurrenceList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

/*

计算商品的共现关系

1. 将用户的购买商品列表进行分割
2. 商品与商品之间 两两共现， 自身与自身也算
*/
public class GoodsCooCurrenceList  extends Configured implements Tool{
    public int run(String[] strings) throws Exception {

        Configuration conf = getConf();
        Path  in = new Path(conf.get("in"));
        Path  out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());

        ///mapper 设置

        job.setMapperClass(GoodsCooCurrenceListMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);


        //设置reducer

        job.setReducerClass(GoodsCooCurrenceListReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    static  class  GoodsCooCurrenceListMapper extends Mapper<LongWritable,Text,Text,Text>{
        Text outkey = new Text();
        Text outvalue = new Text();
        // 10001  34,34,54,656
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] goods = value.toString().split("\t")[1].split(",");
            for (String a : goods) {
                for (String b : goods) {
                    outkey.set(a);
                    outvalue.set(b);
                    context.write(outkey,outvalue);
                }
            }
        }
    }

    static  class GoodsCooCurrenceListReducer extends Reducer<Text,Text,Text,Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key,value);
            }
        }
    }


    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new GoodsCooCurrenceList(),args));
    }


}
