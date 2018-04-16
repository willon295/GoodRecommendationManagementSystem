package cn.willon.bigdata.grms.s7_DuplicateDataFromResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class DuplicateDataFromResult extends Configured implements Tool {
    public int run(String[] strings) throws Exception {


        Configuration conf = getConf();
        Path rawin = new Path(conf.get("rawin"));
        Path resin = new Path(conf.get("resin"));
        Path out = new Path(conf.get("out"));
        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());

        //job.setMapperClass(DuplicateDataFromResultForRawDataMapper.class);
        //job.setMapperClass(DuplicateDataFromResultForRecommandResultMapper.class);
        MultipleInputs.addInputPath(job, rawin, TextInputFormat.class, DuplicateDataFromResultForRawDataMapper.class);
        MultipleInputs.addInputPath(job, resin, TextInputFormat.class, DuplicateDataFromResultForRecommandResultMapper.class);


        //设置 reducer
        job.setReducerClass(DuplicateDataFromResultReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);


        return job.waitForCompletion(true) ? 0 : 1;

    }


    //处理原始数据 mapper
    static class DuplicateDataFromResultForRawDataMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text keyOut = new Text();
        Text valueOut = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            //split[0] => 用户 split[1] => 物品  split[2] => 是否1
            String k = split[0] + "\t" + split[1];
            keyOut.set(k);
            valueOut.set("@" + split[2]);
            context.write(keyOut, valueOut);

            // 10001  20001   =>  @1
        }
    }


    //处理推荐结果数据 mapper
    static class DuplicateDataFromResultForRecommandResultMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text keyOut = new Text();
        Text valueOut = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");
            String[] split1 = split[0].split(",");
            String k = split1[0] + "\t" + split1[1];
            String sum = split[1];
            keyOut.set(k);
            valueOut.set("#" + sum);
            context.write(keyOut, valueOut);
        }
    }

    static class DuplicateDataFromResultReducer extends Reducer<Text, Text, Text, Text> {


        //  1001  2001  =>  [@1 , #3 ]

        Text valueOut = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            boolean haveBought = false;
            for (Text value : values) {
                char c = value.toString().charAt(0);
                if (c == '@') {
                    haveBought = true;
                } else if (c == '#') {
                    String s = value.toString().split("#")[1];
                    valueOut.set(s);
                }
            }

            if (!haveBought) {
                context.write(key, valueOut);
            }

        }
    }

    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new DuplicateDataFromResult(), args));
    }

}
