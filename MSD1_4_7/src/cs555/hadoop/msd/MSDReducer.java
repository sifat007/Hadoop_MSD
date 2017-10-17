package cs555.hadoop.msd;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
import java.io.IOException;
import java.util.HashSet;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class MSDReducer extends Reducer<Text, MapWritable, Text, Text> {
	private static IntWritable one = new IntWritable(1);
	private static IntWritable two = new IntWritable(2);
	private static IntWritable three = new IntWritable(3);
	private static IntWritable four = new IntWritable(4);
	private static Text result = new Text();
	private static HashSet<String> set = new HashSet<String>();
	
	private Map<Text, DoubleWritable> tempoMap = new HashMap<>();
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        String genre = "";
        double max = 0;
        double max_tempo = 0;
        for(MapWritable map : values){
        	String genre_t = ((Text)map.get(one)).toString();
        	double max_t = ((DoubleWritable)map.get(two)).get();
        	if(max_t > max) {
        		genre = genre_t;
        		max = max_t;
        	}else if(max_t == max) {
        		if(genre.equals(genre_t)) {
        			genre = genre_t;
        		}else {        			
        			set.clear();
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
        	int count = ((IntWritable)map.get(three)).get();        	
        	total += count;
        	double tempo = ((DoubleWritable)map.get(four)).get();
        	if(tempo > max_tempo) {
        		max_tempo = tempo;
        	}
        }       
        //result.set(total + "\t" + genre);
        //context.write(key, result);
        
        tempoMap.put(new Text(key + "\t" + total + "\t" + genre), new DoubleWritable(max_tempo));
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Map<Text, DoubleWritable> sortedMap = sortByValues(tempoMap);

        int counter = 0;
        for (Text key: sortedMap.keySet()) {
            //if (counter ++ == 10) {
            //    break;
            //}
            context.write(key, new Text("\t"+sortedMap.get(key).get()));
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
