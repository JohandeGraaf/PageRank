package nl.hu.hadoop.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankAlgorithm {
    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s");
            Node n = Node.fromString(tokens[0], tokens[1]);

            Double p = n.getAdjacentNodes().size() > 0 ? n.getNoderank() / n.getAdjacentNodes().size() : 0;
            context.write(new Text(n.getNodeid()), new Text("#"+n.toString()));

            for(String m : n.getAdjacentNodes()) {
                context.write(new Text(m), new Text(p.toString()));
            }
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        private double alpha = 0;
        private int totalNodes = Integer.MAX_VALUE;
        private double pagerankLost = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            alpha = context.getConfiguration().getDouble("alpha", 0);
            totalNodes = context.getConfiguration().getInt("totalNodes", Integer.MAX_VALUE);
            pagerankLost = context.getConfiguration().getDouble("pagerankLost", 0);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Node m = null;
            double sum = 0;

            for(Text str : values) {
                if(str.toString().startsWith("#")) {
                    m = Node.fromString(key.toString(), str.toString().substring(1));
                } else {
                    sum += Double.parseDouble(str.toString());
                }
            }

            double random_jump = this.alpha * (1D / this.totalNodes);
            double pagerank = (1-this.alpha) * ((this.pagerankLost / this.totalNodes) + sum);

            if(m != null) {
                m.setNoderank(random_jump + pagerank);
                context.write(new Text(m.getNodeid()), new Text(m.toString()));
            }
        }
    }
}
