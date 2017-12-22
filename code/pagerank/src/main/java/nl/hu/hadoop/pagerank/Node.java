package nl.hu.hadoop.pagerank;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class Node {
    public static Node fromString(String nodeid, String str) {
        Node n = new Node();
        n.nodeid = nodeid;
        String[] tokens = str.split(":");

        n.noderank = Double.parseDouble(tokens[0]);

        if(tokens.length > 1) {
            String[] nodelist = tokens[1].split(";");
            n.adjacentNodes.addAll(Arrays.asList(nodelist));
        }

        return n;
    }

    private String nodeid;
    private double noderank;
    private List<String> adjacentNodes;

    public Node() {
        adjacentNodes = new ArrayList<>();
    }

    public Node(String nodeid, double noderank, String... adjacentNodes) {
        this.nodeid = nodeid;
        this.noderank = noderank;
        this.adjacentNodes = new ArrayList<>(Arrays.asList(adjacentNodes));
    }

    public String getNodeid() {
        return nodeid;
    }

    public void setNodeid(String nodeid) {
        this.nodeid = nodeid;
    }

    public double getNoderank() {
        return noderank;
    }

    public void setNoderank(double noderank) {
        this.noderank = noderank;
    }

    public List<String> getAdjacentNodes() {
        return adjacentNodes;
    }

    public void setAdjacentNodes(List<String> adjacentNodes) {
        this.adjacentNodes = adjacentNodes;
    }

    @Override
    public String toString() {
        return String.format(Locale.US, "%f:%s", this.noderank, String.join(";", this.adjacentNodes));
    }
}
