package example;

import java.io.IOException;
import java.util.Objects;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class InvertedIndexMapper extends Mapper<Object , Text, Text, Text>
{
    /*K1，V1 to K2，V2
    key: K1 line_offset
    value: V1 each line's text data
    context:表示上下文对象
    */
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
    {
        FileSplit fileSplit = (FileSplit)context.getInputSplit();//split
        String fileName = fileSplit.getPath().getName();//get file's name
        Text word_file = new Text();//reserve K2(word:filename)
        String lower_line = value.toString().toLowerCase();// turn the word from bigger_case to lower_case
        String value_array = lower_line.replaceAll("[^a-z0-9]"," ");// remove punctuation,replaced by blank space
        StringTokenizer itr = new StringTokenizer(value_array);//split by blank space
        while (itr.hasMoreTokens()) {
            String str = itr.nextToken()+":"+fileName;//combine word and filename
            word_file.set(str);//K2
            Text info =new Text();
            info.set(new LongWritable(1).toString());
            context.write(word_file, info);
        }
    }
}

