package nl.hu.hadoop.pagerank;

import nl.hu.hadoop.pagerank.PageRankAlgorithm.PageRankMapper;
import nl.hu.hadoop.pagerank.PageRankAlgorithm.PageRankReducer;
import nl.hu.hadoop.pagerank.PageRankInputParse.PageRankParseMapper;
import nl.hu.hadoop.pagerank.PageRankInputParse.PageRankParseReducer;
import nl.hu.hadoop.pagerank.PageRankPreAlgorithm.PageRankPreMapper;
import nl.hu.hadoop.pagerank.PageRankPreAlgorithm.PageRankPreReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PageRank {
    //Get project directory ex. "C:\Users\Johan\Dropbox\School\Jaar3\BlokB\BDSD\tcifbigdata\"
    private static final String BASE_PATH;
    static {
        BASE_PATH = System.getProperty("user.dir")+"\\";
        System.out.println("Project dir: " + BASE_PATH);
    }

    private static final String INPUT_PATH = BASE_PATH + "input\\";
    private static final String PARSE_PATH = BASE_PATH + "output\\init_state\\";
    private static final String PRE_PATH = BASE_PATH + "output\\pre\\";
    private static final String OUTPUT_PATH = BASE_PATH + "output\\";

    public static void main(String[] args) throws Exception {
        PageRank pr = new PageRank();
        pr.parseGraph();
        String lastOutputPath = pr.runPageRank();
        if(lastOutputPath != null)
            pr.analyseOutput(lastOutputPath);
    }

    private final double alpha = 0.2;
    private final int pageRankIterations = 10;
    private int totalNodes = 0;

    public void parseGraph() throws Exception {
        System.out.println("--- Begin parsing graph ---");

        //Delete output directory.
        java.nio.file.Path outputDir = Paths.get(PARSE_PATH);
        if(Files.exists(outputDir))
            Files.walk(outputDir)
                    .sorted(Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(File::delete);

        Job job = Job.getInstance();
        job.setJarByClass(PageRankInputParse.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(PARSE_PATH));

        job.setMapperClass(PageRankParseMapper.class);
        job.setReducerClass(PageRankParseReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        TimeUnit.SECONDS.sleep(3);

        //Get total number of nodes in the network.  This is necessary to eventually calculate the random jump factor and to redistribute lost pagerank.
        this.totalNodes = Files.walk(outputDir)
                .filter(p -> !p.toFile().isDirectory())
                .filter(p -> p.toFile().getName().startsWith("part-r-"))
                .flatMap(p -> {
                    try {
                        return Stream.of(new String(Files.readAllBytes(p)).split("\\n"));
                    } catch (IOException e) {
                        return Stream.empty();
                    }
                })
                .map(l -> l.split("\\t")[0].trim())
                .filter(l -> !l.isEmpty())
                .collect(Collectors.toCollection(HashSet::new))
                .size();

        System.out.println("Total nodes in network: " + this.totalNodes);
        System.out.println("--- End parsing graph ---");
    }

    public String runPageRank() throws Exception {
        System.out.println("--- Begin PageRank ---");

        String lastOutput = null;

        int j = new File(OUTPUT_PATH).list().length-1;
        for(int i = j; i < j + this.pageRankIterations; i++) {
            double pagerankLost = checkDeadNodes((i == j ? PARSE_PATH : OUTPUT_PATH+(i-1)), PRE_PATH+i);

            System.out.printf("PageRank iteration #%d\t\t", (i-j+1));
            if(pagerankLost > 0.0D) System.out.printf("PageRank redistributed from dead nodes: %.3f", pagerankLost);
            System.out.println();

            runPageRankJob((i == j ? PARSE_PATH : OUTPUT_PATH+(i-1)), OUTPUT_PATH+i, pagerankLost);
            lastOutput = OUTPUT_PATH+i;
            TimeUnit.SECONDS.sleep(3);
        }

        System.out.println("--- End PageRank ---");
        return lastOutput;
    }

    public double checkDeadNodes(String inputPath, String outputPath) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(PageRankPreAlgorithm.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(PageRankPreMapper.class);
        job.setReducerClass(PageRankPreReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        TimeUnit.SECONDS.sleep(2);

        //Get lost pagerank value.
        return Files.walk(Paths.get(outputPath))
                .filter(p -> !p.toFile().isDirectory())
                .filter(p -> p.toFile().getName().startsWith("part-r-"))
                .flatMap(p -> {
                    try {
                        return Stream.of(new String(Files.readAllBytes(p)).split("\\n"));
                    } catch (IOException e) {
                        return Stream.empty();
                    }
                })
                .map(String::trim)
                .filter(l -> !l.isEmpty())
                .map(l -> l.split("\\t")[1])
                .mapToDouble(Double::parseDouble)
                .sum();
    }

    public void runPageRankJob(String inputPath, String outputPath, double pagerankLost) throws Exception {
        Configuration conf = new Configuration();
        conf.setDouble("alpha", this.alpha);
        conf.setInt("totalNodes", this.totalNodes);
        conf.setDouble("pagerankLost", pagerankLost);

        Job job = Job.getInstance(conf);
        job.setJarByClass(PageRankAlgorithm.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }

    public void analyseOutput(String input) throws Exception {
        System.out.println("--- Sorted output ---");
        System.out.println("NodeID \t\tPageRank Score");

        Files.walk(Paths.get(input))
                .filter(p -> !p.toFile().isDirectory())
                .filter(p -> p.toFile().getName().startsWith("part-r-"))
                .flatMap(p -> {
                    try {
                        return Stream.of(new String(Files.readAllBytes(p)).split("\\n"));
                    } catch (IOException e) {
                        return Stream.empty();
                    }
                })
                .map(String::trim)
                .filter(l -> !l.isEmpty())
                .map(l -> {
                    String[] tokens = l.split("\\t");
                    return Node.fromString(tokens[0], tokens[1]);
                })
                .sorted(Comparator.comparingDouble(Node::getNoderank).reversed())
                .limit(20)
                .forEach(n -> System.out.printf(Locale.US, "%-6s\t\t%.8f\n", n.getNodeid(), n.getNoderank()));
    }
}
