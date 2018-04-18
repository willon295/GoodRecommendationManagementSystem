package cn.willon.bigdata.grms.s1_GetUserBuyGoodList;

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

/**
 * 生成用户购买列表
 * */
public class UserBuyGoodList extends Configured implements Tool {


    public int run(String[] strings) throws Exception {

        Configuration conf = getConf();
        conf.set("stream.reduce.output.field.separator",",");
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());

        //mapper
        job.setMapperClass(UserBuyGoodListMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        //reducer

        job.setReducerClass(UserBuyGoodListReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static class UserBuyGoodListMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text user = new Text();
        Text good = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] s = value.toString().split("\t");
            user.set(s[0]);
            good.set(s[1]);
            context.write(user,good);
        }
    }


    public static class UserBuyGoodListReducer extends Reducer<Text, Text, Text, Text> {

        Text goodList = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuffer sb =new StringBuffer();
            for (Text value : values) {
                sb.append(value.toString()).append(",");
            }
            goodList.set(sb.toString().substring(0,sb.length()-1));
            context.write(key,goodList);
        }
    }


    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new UserBuyGoodList(), args));
    }


}
