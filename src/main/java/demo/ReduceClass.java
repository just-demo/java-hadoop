package demo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import static java.util.stream.StreamSupport.stream;

public class ReduceClass extends Reducer <Text, IntWritable, Text, IntWritable>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = stream(values.spliterator(), false)
                .mapToInt(IntWritable::get)
                .sum();
        context.write(key, new IntWritable(sum));
    }
}