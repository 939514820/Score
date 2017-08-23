import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class AverageScore {
  public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreElements()) {//读入每一行数据
                String strName = tokenizer.nextToken();  // 姓名
                String strScore = tokenizer.nextToken();// 成绩
                context.write(new Text(strName), new IntWritable(Integer.parseInt(strScore)));
            }

        }
    }
//张三：{40,50,90}
    public static class Reduce extends
            Reducer<Text, IntWritable, Text, IntWritable> {
       public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                sum += iterator.next().get();// 总分
                count++;// 总的科目数
            }
            int average = (int) sum / count;// 平均成绩
            context.write(key, new IntWritable(average));
        }
    }
//张三：50

    public static void main(String[] args) throws Exception {

        Configuration conf=new Configuration();
        Job job = new Job(conf, "Score Average");
        job.setJarByClass(AverageScore.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.47.11:9000/input"));
//        FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.47.11:9000/output"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}