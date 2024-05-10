package example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        try {
            Configuration conf = new Configuration();

            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: main <in> <out>");
                System.exit(2);
            }
//            // 自定义输入路径
//            String inputPath = "test/Shakespeare";
//            // 自定义输出路径
//            String outputPath = "output/";
            Job job = new Job(conf, "invert index");
            job.setJarByClass(Main.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(InvertedIndexMapper.class);
            job.setCombinerClass(InvertedIndexCombiner.class);
            job.setReducerClass(InvertedIndexReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

//            FileInputFormat.addInputPath(job, new Path(inputPath));
//            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            // 设置输入文件的路径
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            // 设置输出文件的路径
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}