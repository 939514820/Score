import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by Administrator on 2017/1/19.
 */
public class MaxScore extends Configured implements Tool {
    public static final String TAB_SEPARATOR=" ";
    public static class GenderMapper extends Mapper<LongWritable, Text, Text, Text> {
        /*
         * 调用map解析一行数据，该行的数据存储在value参数中，然后根据\t分隔符，解析出姓名，年龄，性别和成绩
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           String[] tokens = value.toString().split(" ");
            String ScoreandName = tokens[0] + " " + tokens[1] ;
           context.write(new Text("max"), new Text(ScoreandName));
        }
    }


    public static class GenderCombiner extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
            int maxScore = Integer.MIN_VALUE;
            int score = 0;
            String name = " ";

            for (Text val : values) {
                String[] valTokens = val.toString().split(TAB_SEPARATOR);
                score = Integer.parseInt(valTokens[1]);
                if (score > maxScore) {
                    name = valTokens[0];
                    maxScore = score;
                }
            }

            context.write(key, new Text(name + TAB_SEPARATOR + maxScore));
        }
    }



    public static class GenderReducer extends Reducer<Text, Text, Text, Text>  {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maxScore = Integer.MIN_VALUE;
            int score = 0;
            String name = " ";


            // 根据key，迭代 values集合，求出最高分
            for (Text val : values) {
                String[] valTokens = val.toString().split(TAB_SEPARATOR);
                score = Integer.parseInt(valTokens[1]);
                if (score > maxScore) {
                    name = valTokens[0];
                    maxScore = score;
                }
            }

            context.write(new Text(name), new Text("score：" + maxScore));
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public int run(String[] args) throws Exception {
        // 读取配置文件
        Configuration conf = new Configuration();

        Path mypath = new Path(args[1]);
        FileSystem hdfs = mypath.getFileSystem(conf);
        if (hdfs.isDirectory(mypath)) {
            hdfs.delete(mypath, true);
        }

        // 新建一个任务
        Job job = new Job(conf, "gender");
        // 主类
        job.setJarByClass(MaxScore.class);
        // Mapper
        job.setMapperClass(GenderMapper.class);
        // Reducer
        job.setReducerClass(GenderReducer.class);

        // map 输出key类型
        job.setMapOutputKeyClass(Text.class);
        // map 输出value类型
        job.setMapOutputValueClass(Text.class);

        // reduce 输出key类型
        job.setOutputKeyClass(Text.class);
        // reduce 输出value类型
        job.setOutputValueClass(Text.class);

        // 设置Combiner类
        job.setCombinerClass(GenderCombiner.class);


       job.setNumReduceTasks(1);

        // 输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交任务
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        String[] args0 = {
                "hdfs://192.168.237.11:9000/input/score.txt",
                "hdfs://192.168.237.11:9000/output/score/max/"
        };
        int ec = ToolRunner.run(new Configuration(),new MaxScore(), args0);
        System.exit(ec);
    }
}