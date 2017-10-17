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
public class MSDReducer extends Reducer<Text, SortedMapWritable, Text, Text> {
	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable two = new IntWritable(2);
	private MapWritable record = new MapWritable();
	
	private SortedMapWritable  sortedMap = new SortedMapWritable();
    @Override
    protected void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {

        for(SortedMapWritable map : values){
        	sortedMap.putAll(map);
        }   
        
        int limit = Math.min(sortedMap.size(),10);
        for(int i = 0 ; i < limit ; i++) {
        	WritableComparable hotness = sortedMap.lastKey();
        	double hotness1 = ((DoubleWritable)hotness).get();
            String detail = ((Text)sortedMap.get(hotness)).toString();
            context.write(key, new Text(hotness1 + "\t" + detail));
            sortedMap.remove(hotness);
        } 
        
    }
    
   
    
}
