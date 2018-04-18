package cn.willon.bigdata.grms.s6_CombineRecommandResult;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * 统计零散的推荐结果
 */
public class MakeSumForMultiplication extends Configured implements Tool {
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Path  in = new Path(conf.get("in"));
        Path  out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());


        //mapper

        job.setMapperClass(MakeSumForMultiplicationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);


        //reducer
        job.setReducerClass(MakeSumForMultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);


        return  job.waitForCompletion(true)?0:1;
    }


    //
    public static class MakeSumForMultiplicationMapper extends Mapper<Object, Text, Text, IntWritable> {


        Text keyOut = new Text();
        IntWritable valurOut = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");
            keyOut.set(split[0]);
            valurOut.set(Integer.parseInt(split[1]));
            context.write(keyOut, valurOut);
        }
    }

    public static class MakeSumForMultiplicationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        IntWritable valurOut = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            valurOut.set(sum);
            context.write(key, valurOut);
        }
    }

    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new MakeSumForMultiplication(),args));
    }
}
