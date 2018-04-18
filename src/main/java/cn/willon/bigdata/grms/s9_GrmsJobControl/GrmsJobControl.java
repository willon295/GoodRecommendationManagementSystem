package cn.willon.bigdata.grms.s9_GrmsJobControl;

import cn.willon.bigdata.grms.s1_GetUserBuyGoodList.UserBuyGoodList;
import cn.willon.bigdata.grms.s2_GoodsCooCurrenceList.GoodsCooCurrenceList;
import cn.willon.bigdata.grms.s3_GoodsCooCurrenceTimes.GoodsCooCurrenceTimes;
import cn.willon.bigdata.grms.s4_UserBuyGoodsVector.UserBuyGoodsVector;
import cn.willon.bigdata.grms.s5_MultiplyGoodListAndUserVector.MultiplyGoodListAndUserVector;
import cn.willon.bigdata.grms.s6_CombineRecommandResult.MakeSumForMultiplication;
import cn.willon.bigdata.grms.s7_DuplicateDataFromResult.DuplicateDataFromResult;
import cn.willon.bigdata.grms.s8_SaveRecommandResult2DB.ResultKey;
import cn.willon.bigdata.grms.s8_SaveRecommandResult2DB.SaveRecommandResult2DB;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Properties;


/**
 * 作业控制 ， 将所有步骤的 job 串行， 构成一个完整的工作流
 * 输入：  原始数据
 * 输出：  推荐结果 => MySQL
 */
public class GrmsJobControl extends Configured implements Tool {
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        //设置所有  job的输入输出路径
        Path s1in = new Path(conf.get("s1in"));  //job1 输入是   原始数据
        Path s1out = new Path(conf.get("s1out"));
        Path s2out = new Path(conf.get("s2out")); //job2 输入是   job1 的 输出
        Path s3out = new Path(conf.get("s3out")); //job3 输入是   job2 的 输出
        Path s4out = new Path(conf.get("s4out")); //job4 输入是   job1 的 输出
        Path s5out = new Path(conf.get("s5out")); //job5 输入是   job3 ,job4 输出
        Path s6out = new Path(conf.get("s6out")); //job6 输入是   job5 的输出
        Path s7out = new Path(conf.get("s7out")); //job7 输入是   job6 的输出、原始数据
        //job8 的输入是  job7  的输出

        //获取 8 个job 对象
        Job job1 = Job.getInstance(conf,"job-001");
        Job job2 = Job.getInstance(conf,"job-002");
        Job job3 = Job.getInstance(conf,"job-003");
        Job job4 = Job.getInstance(conf,"job-004");
        Job job5 = Job.getInstance(conf,"job-005");
        Job job6 = Job.getInstance(conf,"job-006");
        Job job7 = Job.getInstance(conf,"job-007");
        Job job8 = Job.getInstance(conf,"job-008");

        //配置 8 个job  M R 信息
        //job1 配置
        job1.setJarByClass(this.getClass());
        job1.setMapperClass(UserBuyGoodList.UserBuyGoodListMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job1, s1in);
        job1.setReducerClass(UserBuyGoodList.UserBuyGoodListReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1, s1out);

        //job2 配置
        job2.setJarByClass(this.getClass());
        job2.setMapperClass(GoodsCooCurrenceList.GoodsCooCurrenceListMapper.class);
        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job2, s1out);
        job2.setReducerClass(GoodsCooCurrenceList.GoodsCooCurrenceListReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2, s2out);

        //job3配置
        job3.setJarByClass(this.getClass());
        job3.setMapperClass(GoodsCooCurrenceTimes.GoodsCooCurrenceTimesMapper.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setMapOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(job3, s2out);
        job3.setReducerClass(GoodsCooCurrenceTimes.GoodsCooCurrenceTimesReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job3, s3out);


        //job4配置
        job4.setJarByClass(this.getClass());
        job4.setMapperClass(UserBuyGoodsVector.UserBuyGoodsVectorMapper.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(IntWritable.class);
        job4.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job4, s1out);
        job4.setReducerClass(UserBuyGoodsVector.UserBuyGoodsVectorReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job4, s4out);


        //job5 配置
        job5.setJarByClass(this.getClass());
        job5.setMapperClass(MultiplyGoodListAndUserVector.MultiplyGoodListAndUserVectorMapper1.class);
        job5.setMapperClass(MultiplyGoodListAndUserVector.MultiplyGoodListAndUserVectorMapper2.class);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);
        job5.setInputFormatClass(TextInputFormat.class);
        MultipleInputs.addInputPath(job5, s3out, TextInputFormat.class, MultiplyGoodListAndUserVector.MultiplyGoodListAndUserVectorMapper1.class);
        MultipleInputs.addInputPath(job5, s4out, TextInputFormat.class, MultiplyGoodListAndUserVector.MultiplyGoodListAndUserVectorMapper2.class);
        job5.setReducerClass(MultiplyGoodListAndUserVector.MultiplyGoodListAndUserVectorReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);
        job5.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job5, s5out);

        //job6配置
        job6.setJarByClass(this.getClass());
        job6.setMapperClass(MakeSumForMultiplication.MakeSumForMultiplicationMapper.class);
        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(IntWritable.class);
        job6.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job6, s5out);
        job6.setReducerClass(MakeSumForMultiplication.MakeSumForMultiplicationReducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(IntWritable.class);
        job6.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job6, s6out);


        //job7配置
        job7.setJarByClass(this.getClass());
        MultipleInputs.addInputPath(job7, s1in, TextInputFormat.class, DuplicateDataFromResult.DuplicateDataFromResultForRawDataMapper.class);
        MultipleInputs.addInputPath(job7, s6out, TextInputFormat.class, DuplicateDataFromResult.DuplicateDataFromResultForRecommandResultMapper.class);
        job7.setReducerClass(DuplicateDataFromResult.DuplicateDataFromResultReducer.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(Text.class);
        job7.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job7, s7out);

        //job8 配置
        job8.setJarByClass(this.getClass());
        job8.setMapperClass(SaveRecommandResult2DB.SaveRecommandResult2DBMapper.class);
        job8.setMapOutputKeyClass(Text.class);
        job8.setMapOutputValueClass(NullWritable.class);
        job8.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job8, s7out);
        job8.setReducerClass(SaveRecommandResult2DB.SaveRecommandResult2DBReducer.class);
        job8.setOutputKeyClass(ResultKey.class);
        job8.setOutputValueClass(NullWritable.class);
        job8.setOutputFormatClass(DBOutputFormat.class);
        Properties p = new Properties();
        p.load(this.getClass().getResourceAsStream("/db.properties"));
        DBConfiguration.configureDB(job8.getConfiguration(), p.getProperty("driver"), p.getProperty("jdbcUrl"), p.getProperty("username"), p.getProperty("password"));
        DBOutputFormat.setOutput(job8, p.getProperty("table"), "uid", "gid", "exp");


        //将 8 个job 转换成  可控制 job
        ControlledJob cjob1 = new ControlledJob(conf);
        cjob1.setJob(job1);
        ControlledJob cjob2 = new ControlledJob(conf);
        cjob2.setJob(job2);
        ControlledJob cjob3 = new ControlledJob(conf);
        cjob3.setJob(job3);
        ControlledJob cjob4 = new ControlledJob(conf);
        cjob4.setJob(job4);
        ControlledJob cjob5 = new ControlledJob(conf);
        cjob5.setJob(job5);
        ControlledJob cjob6 = new ControlledJob(conf);
        cjob6.setJob(job6);
        ControlledJob cjob7 = new ControlledJob(conf);
        cjob7.setJob(job7);
        ControlledJob cjob8 = new ControlledJob(conf);
        cjob8.setJob(job8);


        //添加job 之间依赖关系
        cjob2.addDependingJob(cjob1);
        cjob3.addDependingJob(cjob2);
        cjob4.addDependingJob(cjob1);
        cjob5.addDependingJob(cjob3);
        cjob5.addDependingJob(cjob4);
        cjob6.addDependingJob(cjob5);
        cjob7.addDependingJob(cjob6);
        cjob8.addDependingJob(cjob7);


        //创建 jobControl 对象

        JobControl jc = new JobControl("jc");
        jc.addJob(cjob1);
        jc.addJob(cjob2);
        jc.addJob(cjob3);
        jc.addJob(cjob4);
        jc.addJob(cjob5);
        jc.addJob(cjob6);
        jc.addJob(cjob7);
        jc.addJob(cjob8);

        //创建线程，运行
        new Thread(jc).start();

        while (true) {
            if (jc.allFinished()) {
                jc.stop();
                return 0;
            } else if (jc.getFailedJobList().size() > 0) {
                jc.stop();
                return 1;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GrmsJobControl(),args));

    }
}
