package cn.willon.bigdata.grms.s8_SaveRecommandResult2DB;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Properties;


/**
 * 数据入库，写入 MySQL 数据库
 * */
public class SaveRecommandResult2DB extends Configured implements Tool {
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Path in = new Path(conf.get("in"));

        Properties p = new Properties();
        p.load(this.getClass().getResourceAsStream("/db.properties"));

        Job job = Job.getInstance(conf, "saveResult2DB");
        job.setJarByClass(this.getClass());
        job.setMapperClass(SaveRecommandResult2DBMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, in);

        //设置reducer
        job.setReducerClass(SaveRecommandResult2DBReducer.class);
        job.setOutputKeyClass(ResultKey.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        //设置数据库连接信息

        // !!! 特别注意， 这里的  `configuration` 一定要从 job里获取， 否则会少配置信息， 报  DBOutPutFormat.getWriter 异常
        DBConfiguration.configureDB(job.getConfiguration(), p.getProperty("driver"), p.getProperty("jdbcUrl"), p.getProperty("username"), p.getProperty("password"));
        //设置数据表， 要插入的列
        DBOutputFormat.setOutput(job, p.getProperty("table"), "uid", "gid", "exp");

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class SaveRecommandResult2DBMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            context.write(value, NullWritable.get());
        }
    }

    public static class SaveRecommandResult2DBReducer extends Reducer<Text, NullWritable, ResultKey, NullWritable> {

        ResultKey keyOt = new ResultKey();

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String[] split = key.toString().split("\t");
            keyOt.setUid(split[0]);
            keyOt.setGid(split[1]);
            keyOt.setExp(Integer.parseInt(split[2]));
            context.write(keyOt, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new SaveRecommandResult2DB(), args));
    }
}
