package nl.hu.hadoop.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankPreAlgorithm {
    public static class PageRankPreMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s");
            Node n = Node.fromString(tokens[0], tokens[1]);

            if(n.getAdjacentNodes().isEmpty())
                context.write(new Text("lost-pagerank"), new Text(Double.toString(n.getNoderank())));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("lost-pagerank"), new Text(Double.toString(0D)));
        }
    }

    public static class PageRankPreReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0D;
            for(Text str: values) {
                sum += Double.parseDouble(str.toString());
            }

            context.write(key, new Text(Double.toString(sum)));
        }
    }
}
