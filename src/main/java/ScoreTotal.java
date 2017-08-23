import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class ScoreTotal {

    /**
     * Mapper
     */
    //每个map统计最大值或者最小值，比当前最大值大就统计为最大值
    //比最小值小就统计为最小值
    //每个map任务最后产生结果为姓名，分数（包含两条记录,成绩相同则产生多条）


    public static class Map extends Mapper<Object, Text, Text, Text> {
        private int max = Integer.MIN_VALUE;
        private int min = Integer.MAX_VALUE;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // 将输入的数据首先按行进行分割
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreElements()) {
                String strName = tokenizer.nextToken();  // 姓名
                String strScore = tokenizer.nextToken();// 成绩
                if (Integer.parseInt(strScore) > max) {
                    max = Integer.parseInt(strScore);
                    context.write(new Text("max "), new Text(String.valueOf(key+":")+max));
                } else if (Integer.parseInt(strScore) < min) {
                    min = Integer.parseInt(strScore);
                    context.write(new Text("min "), new Text(String.valueOf(key+":")+min));
                } else {
                    if (Integer.parseInt(strScore) == max) {
                        max = Integer.parseInt(strScore);
                        context.write(new Text("max "), new Text(String.valueOf(key+":")+max));
                    } else if (Integer.parseInt(strScore) == min) {
                        min = Integer.parseInt(strScore);
                        context.write(new Text("min "), new Text(String.valueOf(key+":")+min));
                    }
                }


            }

        }
    }
    //max {张三:100, 李四:100,赵六：90}
//max
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        int tempscore = 0;
        Text tempkey =null;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();  //循环遍历成绩
            boolean flag = true;
            while (iterator.hasNext()) {
                String tempstr=iterator.next().toString();//李四:100
                int middle=tempstr.indexOf(":");
                tempscore = Integer.parseInt(tempstr.substring(middle));//获得成绩
                //成绩相等时
                if (key.toString().contains("max ") && max == tempscore) {
                    context.write(key, new Text(tempstr.substring(0,middle)+" "+max));
                } else if (key.toString().contains("min ") && min == tempscore) {
                    context.write(key, new Text(tempstr.substring(0,middle)+" "+min));
                } else {
                    if (key.toString().contains("max ") && max < tempscore) {
                        max = tempscore;
                        tempkey = key;
                    } else if (key.toString().contains("min ") && min > tempscore) {
                        min = tempscore;
                        tempkey = key;
                        flag = false;
                    }
                    context.write(tempkey, new Text(flag == true ? tempstr.substring(0,middle)+" "+max : tempstr.substring(0,middle)+" "+min));
                }
            }

        }

    }




    public static void main(String[] args) throws Exception {

        Configuration conf= new Configuration();
        Job job = new Job(conf, "Score MinAndMax");
        job.setJarByClass(ScoreTotal.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.47.11:9000/input"));
//        FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.47.11:9000/output"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}