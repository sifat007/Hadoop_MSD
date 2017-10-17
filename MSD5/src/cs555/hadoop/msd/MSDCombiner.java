package cs555.hadoop.msd;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class MSDCombiner extends Reducer<Text, SortedMapWritable, Text, SortedMapWritable> {
	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable two = new IntWritable(2);
	
	private SortedMapWritable  sortedMap = new SortedMapWritable();
	private SortedMapWritable  subMap = new SortedMapWritable();
    @Override
    protected void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {

        for(SortedMapWritable map : values){
        	DoubleWritable hotness = (DoubleWritable)map.get(one);
        	Text songdetail = (Text)map.get(two);  
        	sortedMap.put(hotness,songdetail);
        }
        
        int limit = Math.min(sortedMap.size(),10);
        for(int i = 0 ; i < limit ; i++) {
        	WritableComparable hotness = sortedMap.lastKey();
        	subMap.put(hotness,sortedMap.get(hotness));
        	sortedMap.remove(hotness);
        }        
        
        context.write(key,subMap);
        
    }
    
    
}
