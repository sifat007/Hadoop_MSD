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
	private final static DoubleWritable _1 = new DoubleWritable(-1);
	private MapWritable record = new MapWritable();
	
	private SortedMapWritable  sortedMap = new SortedMapWritable();

	private static Map<Text, IntWritable> hashMap = new HashMap<>();
	
    @Override
    protected void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {

    	int total = 0;
        for(SortedMapWritable map : values){
        	total += Integer.parseInt(((Text)map.get(_1)).toString());
        	map.remove(_1);
        	sortedMap.putAll(map);
        }
        if(hashMap.containsKey(key)) {
        	total += hashMap.get(key).get();
        }
        hashMap.put(key, new IntWritable(total));
        
        int limit = Math.min(sortedMap.size(),10);
        for(int i = 0 ; i < limit ; i++) {
        	WritableComparable hotness = sortedMap.lastKey();
        	double hotness1 = ((DoubleWritable)hotness).get();
            String detail = ((Text)sortedMap.get(hotness)).toString();
            context.write(key, new Text(hotness1 + "\t" + detail));
            sortedMap.remove(hotness);
        }         
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	
    	context.write(new Text("--------"), new Text("------------------------------------"));

    	Map<Text, IntWritable> sortedMap = hashMap;

        int counter = 0;
        for (Text key : sortedMap.keySet()) {
            if (counter++ == 10) {
                break;
            }
            Text v = new Text(sortedMap.get(key).get() +"");
            context.write(key, v);
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
