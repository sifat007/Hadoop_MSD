package cs555.hadoop.msd;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.regex.*;
/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class MSDMapper extends Mapper<LongWritable, Text, Text, SortedMapWritable> {
	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable two = new IntWritable(2);
	private SortedMapWritable record = new SortedMapWritable();
	
	private static Text textKey = new Text();
	private static Text songdetail_container = new Text();
	private static DoubleWritable hotness_container = new DoubleWritable();
	
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // get row       
        String [] row = value.toString().split("\t"); 
        if(isNumeric(row[0]) ==false) {
        	return;
        }
        
        double hotness;
        try {
        	hotness = Double.parseDouble(row[42]);
        }catch(Exception ex) {
        	hotness = 0;
        }
        hotness_container.set(hotness);
        
        String songdetail = row[11] + "\t" + row[50];
        songdetail_container.set(songdetail);
        
        String genre_txt = "";
        if(row[13].length() > 4) {
        	genre_txt = row[13].trim().substring(2, row[13].length() - 2).trim();
        }
        String [] genre_list = genre_txt.split(",");
        
        Pattern p = Pattern.compile("\"\".+\"\"");
        for(String genre : genre_list) {
        	if(genre.length() > 0) {        	    
        	    Matcher m = p.matcher(genre);
        	    if(m.find()) {
        	    	String s = m.group();
        	    	genre = s.substring(2, s.length() -2);
        	    }
                record.put(one,hotness_container);
                record.put(two,songdetail_container);
                textKey.set(genre);
                context.write(textKey, record);
        	}        	            
        }
        
    }
    
    public static boolean isNumeric(String str)  
    {  
      try  
      {  
        double d = Double.parseDouble(str);  
      }  
      catch(NumberFormatException nfe)  
      {  
        return false;  
      }  
      return true;  
    }
}
