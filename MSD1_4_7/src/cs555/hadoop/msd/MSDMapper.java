package cs555.hadoop.msd;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.util.*;
/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class MSDMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
	private static IntWritable one = new IntWritable(1);
	private static IntWritable two = new IntWritable(2);
	private static IntWritable three = new IntWritable(3);
	private static MapWritable record = new MapWritable();
	private static Text textKey = new Text();
	private static Text oneText = new Text();
	private static DoubleWritable twoDouble = new DoubleWritable();
	private static DoubleWritable threeDouble = new DoubleWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // get row       
        String [] row = value.toString().split("\t"); 
        if(isNumeric(row[0]) ==false) {
        	return;
        }
        
        double tempo;
        try {
        	tempo = Double.parseDouble(row[47]);
        }catch(Exception ex) {
        	tempo = 0;
        }
        
        String genre_feq_str_list[] = row[14].trim().substring(1, row[14].length() - 1 ).split(",");
        double[] genre_feq_list = new double[genre_feq_str_list.length];
        for (int i = 0; i < genre_feq_list.length; i++) {
        	try {
        		genre_feq_list[i] = Double.parseDouble(genre_feq_str_list[i].trim());
        	}catch(Exception ex) {
        		genre_feq_list[i] = 0;
        	}
        }
        String genre_txt = "";
        if(row[13].length() > 4) {
        	genre_txt = row[13].trim().substring(2, row[13].length() - 2).trim();
        }
        String [] genre_list = genre_txt.split(",");
        
        String list = "";
        double max = 0;
        for(int i = 0 ; i < genre_list.length; i++) {
        	String str = genre_list[i].trim();
        	if(str.length() > 4) {
        		String key1 = str.substring(2, str.length() - 2).trim();
            	if(genre_feq_list[i] > max) {
            		list = key1;
            		max = genre_feq_list[i];
            	}else if(genre_feq_list[i] == max) {
            		list += "," + key1;
            	}   
        	}
        }        
        //System.err.println(list);
        oneText.set(list);
        record.put(one,oneText);
        twoDouble.set(max);
        record.put(two,twoDouble);
        threeDouble.set(tempo);
        record.put(three,threeDouble);
        textKey.set(row[11]);
        // emit artist
        context.write(textKey, record);
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
