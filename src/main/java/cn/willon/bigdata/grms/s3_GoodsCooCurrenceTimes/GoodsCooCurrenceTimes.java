package cn.willon.bigdata.grms.s3_GoodsCooCurrenceTimes;


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
import java.util.HashMap;
import java.util.Map;


/**
 * 计算物品的共现次数
 * 物品  物品 次数
 */
public class GoodsCooCurrenceTimes extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));
        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());

        job.setMapperClass(GoodsCooCurrenceTimesMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job, in);

        //设置reducer

        job.setReducerClass(GoodsCooCurrenceTimesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static class GoodsCooCurrenceTimesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        Text keyOut = new Text();
        IntWritable valueOut = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");
            keyOut.set(split[0]);
            valueOut.set(Integer.parseInt(split[1].trim()));

            context.write(keyOut, valueOut);
        }
    }

    public static class GoodsCooCurrenceTimesReducer extends Reducer<Text, IntWritable, Text, Text> {

        Text valueOut = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            Map<Integer, Integer> m = new HashMap<Integer, Integer>();
            StringBuilder sb = new StringBuilder();
            for (IntWritable value : values) {
                if (m.containsKey(value.get())) {
                    int v = m.get(value.get());
                    v++;
                    m.put(value.get(), v);
                } else {
                    m.put(value.get(), 1);
                }
            }


            for (Map.Entry<Integer, Integer> si : m.entrySet()) {
                sb.append(si.getKey()).append(":").append(si.getValue()).append(",");
            }
            m.clear();

            valueOut.set(sb.substring(0,sb.length()-1));
            context.write(key,valueOut);

        }


    }


    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new GoodsCooCurrenceTimes(), args));
    }

}
