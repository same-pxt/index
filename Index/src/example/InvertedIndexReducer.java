package example;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Object,Text,Object,Text> {
    @Override
    public void reduce(Object key, Iterable<Text> values, Context context )
            throws IOException, InterruptedException
    {
        int num=0;
        long times=0;
        String fileList = new String();
        for (Text value : values) {
            fileList += value.toString() + ";";
            num++;
            int index = value.toString().indexOf(":");
            long t = Long.parseLong(value.toString().substring(index+1));
            times+=t;
        }
        double result = (double) times / num;
        DecimalFormat df = new DecimalFormat("#.##");
        df.setMinimumFractionDigits(2);
        String formattedResult = df.format(result);
        fileList = formattedResult+","+fileList;
        Text ans=new Text();
        ans.set(ans);
        ans.set(fileList);
        context.write(key, ans);
    }
}
