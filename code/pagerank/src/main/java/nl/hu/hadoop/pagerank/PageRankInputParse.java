package nl.hu.hadoop.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PageRankInputParse {
    public static class PageRankParseMapper extends Mapper<LongWritable, Text, Text, Text> {
        private HashMap<String, Node> nodes;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            nodes = new HashMap<>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s");

            if(nodes.containsKey(tokens[0])) {
                nodes.get(tokens[0]).getAdjacentNodes().add(tokens[1]);
            } else {
                nodes.put(tokens[0], new Node(tokens[0], 0, tokens[1]));
            }

            if(!nodes.containsKey(tokens[1]))
                nodes.put(tokens[1], new Node(tokens[1], 0));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Node n : nodes.values()) {
                context.write(new Text(n.getNodeid()), new Text(n.toString()));
            }
        }
    }

    public static class PageRankParseReducer extends Reducer<Text, Text, Text, Text> {
        private HashMap<String, Node> nodes;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            nodes = new HashMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Node n = null;

            try {
                for(Text str: values) {
                    if(n == null) {
                        n = Node.fromString(key.toString(), str.toString());
                    } else {
                        //TODO: Bugged
                        n.getAdjacentNodes().add(str.toString());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            if(n != null)
                nodes.put(key.toString(), n);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Map.Entry<String, Node> entry : nodes.entrySet()) {
                entry.getValue().setNoderank(1D / nodes.size());
                context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
            }
        }
    }
}
