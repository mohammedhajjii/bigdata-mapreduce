package md.hajji.sales;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;


/**
 * type parameters for reducer:
 * Text: The type of the input key, which will be city name.
 * Iterable<DoubleWritable>: the type of the input value, which will be list of prices of soling products
 * Text: The type of the output key, which will be city name.
 * DoubleWritable: the type of the output value, which will be sum of all prices of soling products
 */

public class SaleByCityReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {

        // calculate the sum  of prices:
        double sum = StreamSupport.stream(values.spliterator(), false)
                .mapToDouble(DoubleWritable::get)
                .sum();

        // write the result in the output:
        context.write(key, new DoubleWritable(sum));
    }
}
