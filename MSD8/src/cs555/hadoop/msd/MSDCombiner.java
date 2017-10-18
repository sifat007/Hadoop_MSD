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
public class MSDCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
	private final static IntWritable intContainer = new IntWritable(1);
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {   
        
    	int size = 0;
    	for(IntWritable value : values) {
    	   size++;
    	}
    	intContainer.set(size);
        context.write(key,intContainer);
        
    }
    
    
}
