package example;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.shaded.org.ehcache.impl.internal.store.heap.holders.LookupOnlyOnHeapKey;

public class InvertedIndexCombiner extends Reducer<Object, Text,Object,Text> {
    @Override
    public void reduce(Object key, Iterable<Text> values, Context context )
            throws IOException, InterruptedException
    {
        long sum=0;
        for (Text value : values) {
            sum+= Long.parseLong(value.toString());
        }
        int splitIndex=key.toString().indexOf(":");
        Text info=new Text();
        info.set(key.toString().substring(splitIndex + 1) + ":" + sum);
        Text k=new Text();
        k.set(key.toString().substring(0,splitIndex));
        context.write(k,info);
    }
}
