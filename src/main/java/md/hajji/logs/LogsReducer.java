package md.hajji.logs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class LogsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        // compute the sum of total request
        // or succeeded requests:
        long sum = StreamSupport.stream(values.spliterator(), false)
                .mapToLong(LongWritable::get)
                .sum();

        // write the result in the output file
        context.write(key, new LongWritable(sum));
    }
}
