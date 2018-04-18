package cn.willon.bigdata.grms.s5_MultiplyGoodListAndUserVector;

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
import java.util.HashMap;
import java.util.Map;


/**
 * 计算零散的商品推荐信息
 */
public class MultiplyGoodListAndUserVector extends Configured implements Tool {
    public int run(String[] strings) throws Exception {

        Configuration conf = getConf();

        Path goodListIn = new Path(conf.get("gin"));
        Path userVectorIn = new Path(conf.get("uin"));
        Path out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        // 设置 mapper
        job.setMapperClass(MultiplyGoodListAndUserVectorMapper1.class);
        job.setMapperClass(MultiplyGoodListAndUserVectorMapper2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        MultipleInputs.addInputPath(job, goodListIn, TextInputFormat.class, MultiplyGoodListAndUserVectorMapper1.class);
        MultipleInputs.addInputPath(job, userVectorIn, TextInputFormat.class, MultiplyGoodListAndUserVectorMapper2.class);


        // 设置reducer
        job.setReducerClass(MultiplyGoodListAndUserVectorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new MultiplyGoodListAndUserVector(), args));
    }


    //处理物品 mapper
    public static class MultiplyGoodListAndUserVectorMapper1 extends Mapper<LongWritable, Text, Text, Text> {

        Text valueOut = new Text();
        Text keyOut = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            keyOut.set(split[0]);
            valueOut.set("G" + split[1]);
            context.write(keyOut, valueOut);
        }
    }

    //处理用户购买向量  mapper
    public static class MultiplyGoodListAndUserVectorMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        Text valueOut = new Text();
        Text keyOut = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            keyOut.set(split[0]);
            valueOut.set("U" + split[1]);
            context.write(keyOut, valueOut);
        }
    }

    // 进入reducer 的结果是    （ 20001, [U1001:1 1002:1 ,G20001:3,2002:2,2003:4]     ）
    // 输出为   用户:物品  -->  推荐权重
    public static class MultiplyGoodListAndUserVectorReducer extends Reducer<Text, Text, Text, Text> {

        Text keyOut = new Text();
        Text valueOut = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String user = null;
            String good = null;
            int num = 0;
            Map<String, Integer> um = new HashMap<String, Integer>();
            Map<String, Integer> gm = new HashMap<String, Integer>();

            for (Text value : values) {
                char c = value.toString().charAt(0);
                String[] split = value.toString().substring(1).split(",");
                for (String s : split) {

                    String[] split1 = s.split(":");
                    num = Integer.parseInt(split1[1].trim());
                    if (c == 'U') {
                        user = split1[0];
                        um.put(user, num);
                    } else if (c == 'G') {
                        good = split1[0];
                        gm.put(good, num);
                    }
                }
            }
            for (Map.Entry<String, Integer> umi : um.entrySet()) {
                for (Map.Entry<String, Integer> gmi : gm.entrySet()) {
                    keyOut.set((umi.getKey() + "," + gmi.getKey()));
                    int i = (umi.getValue() * gmi.getValue());
                    valueOut.set(String.valueOf(i));
                    context.write(keyOut, valueOut);
                }
            }

        }
    }
}

