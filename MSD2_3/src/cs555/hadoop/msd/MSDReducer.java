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
public class MSDReducer extends Reducer<Text, MapWritable, Text, Text> {
	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable two = new IntWritable(2);
	private final static Text zero = new Text("0");
	
	Map<Double, Integer> hashMap = new HashMap<Double, Integer>();
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
        	
        	for(Writable x: record_dance1.keySet()) {
        		double d = ((DoubleWritable)x).get();
        		int count = ((IntWritable)record_dance1.get(x)).get();
        		if(hashMap.containsKey(d)) {
        			int p = hashMap.get(d);
        			hashMap.put(d,p+count);
        		}else {
        			hashMap.put(d,count);
        		}
        	}        	        
        }       
        
        TreeMap<Double,Integer> treemap = new TreeMap<>(hashMap);
//        int length = treemap.size();
//        if(length % 2 == 0) {
//        	for(int i =0 ; i < length /2; i++) {
//            	
//            }
//        }else {
//    		for(int i =0 ; i < length /2; i++) {
//            	
//            }
//        }
        
        
        context.write(new Text("Tempo avg: "),new Text(tempo_avg+" | "+ tempo_count));
        context.write(new Text("Dance med: "),new Text(Arrays.asList(hashMap.keySet())+""));
    }    
    
}
