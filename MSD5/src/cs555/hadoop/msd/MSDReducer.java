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
	private MapWritable record = new MapWritable();
	
	private Map<Text, DoubleWritable> hotnessMap = new HashMap<>();
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        // calculate the total count

        for(MapWritable map : values){
        	DoubleWritable hotness = (DoubleWritable)map.get(one);
        	Text songdetail = (Text)map.get(two);
        	hotnessMap.put(songdetail,hotness);
        }
        Map<Text, DoubleWritable> sortedMap = sortByValues(hotnessMap);
        
        int counter = 0;
        for (Text songdetail: sortedMap.keySet()) {
            if (counter ++ == 10) {
                break;
            }
            record.put(one,sortedMap.get(songdetail));  //hotness
            record.put(two,songdetail);  				//songdetail
            double hotness = ((DoubleWritable)sortedMap.get(songdetail)).get();
            String detail = ((Text)songdetail).toString();
            context.write(key, new Text(hotness + "\t" + detail));
        }
        
    }
    
    /**
     * sorts the map by values. Taken from:
     * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
     */
    private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
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
