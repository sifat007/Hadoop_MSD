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
public class MSDCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
	private static IntWritable one = new IntWritable(1);
	private static IntWritable two = new IntWritable(2);
	private static IntWritable three = new IntWritable(3);
	private static IntWritable four = new IntWritable(4);
	private static MapWritable record = new MapWritable();
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        // calculate the total count        
        int total = 0;
        String genre = "";
        double max = 0;
        double max_tempo = 0;
        for(MapWritable map : values){
        	String genre_t = ((Text)map.get(one)).toString();
        	double max_t = ((DoubleWritable)map.get(two)).get();
        	if(max_t > max) {
        		genre = genre_t;
        	}else if(max_t == max) {
        		if(genre.equals(genre_t)) {
        			genre = genre_t;
        			max= max_t;
        		}else {
        			HashSet<String> set = new HashSet<String>();
        			for(String x : genre.split(",")) {
        				set.add(x);
        			}
        			for(String x : genre_t.split(",")) {
        				set.add(x);
        			}
        			genre = "";
        			for(String x : set) {
        				if(x.length() > 0)
        					genre = genre + "," + x;
        			}
        			genre = genre.trim().substring(1);
        		}
        	}
        	total += 1;
        	double tempo = ((DoubleWritable)map.get(three)).get();
        	if(tempo > max_tempo) {
        		max_tempo = tempo;
        	}
        	
        }
        record.put(one,new Text(genre));
        record.put(two,new DoubleWritable(max));
        record.put(three,new IntWritable(total));
        record.put(four,new DoubleWritable(max_tempo));  
        context.write(key, record);
    }
    
}
