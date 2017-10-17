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
public class MSDMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable two = new IntWritable(2);
	private final static Text zero = new Text("0");
	private MapWritable record = new MapWritable();
	private MapWritable record_tempo = new MapWritable();
	private MapWritable record_dance = new MapWritable();
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
        record_tempo.put(new DoubleWritable(tempo),one);
        double dance;
        try {
        	dance = Double.parseDouble(row[21]);
        }catch(Exception ex) {
        	dance = 0;
        }
        record_dance.put(new DoubleWritable(dance),one);
                
        record.put(one,record_tempo);
        record.put(two,record_dance);
        
        context.write(zero, record);
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
