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
public class MSDReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable two = new IntWritable(2);
	private final static IntWritable intContainer = new IntWritable(0);


	private static Map<Text, IntWritable> hashMap = new HashMap<>();
	
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

    	int size = 0;
    	for(IntWritable value : values) {
    	   size++;
    	}
    	intContainer.set(size);
        //context.write(key,intContainer);  
        hashMap.put(new Text(key),new IntWritable(size));
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	
    	Map<Text, IntWritable> sortedMap = sortByValues(hashMap);
        int counter = 0;
        for (Text key : sortedMap.keySet()) {
            if (counter++ == 10) {
                break;
            }
            context.write(key, sortedMap.get(key));
        }  

    }
    
    
    
    public static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
    
    
    
   
    
}
