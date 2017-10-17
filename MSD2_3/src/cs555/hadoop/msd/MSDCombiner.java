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
	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable two = new IntWritable(2);
	private final static Text zero = new Text("0");
	private MapWritable record = new MapWritable();
	private MapWritable record_tempo = new MapWritable();
	private MapWritable record_dance = new MapWritable();
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        // calculate the total count
        MapWritable record = new MapWritable();
        double tempo_avg = 0;
        int tempo_count = 0;
        for(MapWritable map : values){
        	//MapWritable mr = (MapWritable)map.get(zero);
        	MapWritable record_tempo1 = (MapWritable)map.get(one);
        	MapWritable record_dance1 = (MapWritable)map.get(two);
        	for(Writable x: record_tempo1.keySet()) {
        		double d = ((DoubleWritable)x).get();
        		int count = ((IntWritable)record_tempo1.get(x)).get();
        		tempo_avg = (tempo_avg * tempo_count + d*count)/ (tempo_count+count);
        		tempo_count+=count;      		
        	}
        	record_tempo.put(new DoubleWritable(tempo_avg),new IntWritable(tempo_count));
        	
        	for(Writable x: record_dance1.keySet()) {
        		int count = ((IntWritable)record_dance1.get(x)).get();;
        		if(record_dance.containsKey(x)) {
        			int p = ((IntWritable)record_dance.get(x)).get();
        			record_dance.put(x,new IntWritable(p+count));
        		}else {
        			record_dance.put(x,record_dance1.get(x));
        		}
        	}
        	
        	
        }
        record.put(one,record_tempo);
        record.put(two,record_dance);
        
        context.write(zero, record);
    }
    
}
