package org.example;

import java.util.*;

public class Topology {

    private final int vnodes;
    public final Map<Long, Node> nodes;

    public Topology() {
        this.vnodes = 32;
        this.nodes = new HashMap<>();
    }

    public Topology(int vnodes) {
        this.vnodes = vnodes;
        this.nodes = new HashMap<>();
    }

    public void join(Node node) {
        NavigableMap<Integer, Token> ring;
        Map<Token, Node> tokenToNode;
        Map<Node, List<Token>> nodeToTokens;

        if (nodes.isEmpty()) {
            ring = new TreeMap<>();
            tokenToNode = new HashMap<>();
            nodeToTokens = new HashMap<>();
            addTokens(node, ring, tokenToNode, nodeToTokens);
        } else {
            // Select the seed node randomly
            List<Long> nodeIds = new ArrayList<>(nodes.keySet());
            Collections.shuffle(nodeIds);
            Node seed = nodes.get(nodeIds.get(0));

            // Copy the existing metadata
            TokenMetadata tokenMetadata = seed.getTokenMetadata();
            ring = tokenMetadata.getRing();
            tokenToNode = tokenMetadata.getTokenToNode();
            nodeToTokens = tokenMetadata.getNodeToTokens();
            List<Token> tokens = addTokens(node, ring, tokenToNode, nodeToTokens);

            // A new node tries to join the cluster
            final Gossip aNewNodeTriesToJoin = new Gossip(node, NodeStatus.BOOTSTRAPING, tokens);
            nodes.values().forEach(n -> n.mergeGossip(aNewNodeTriesToJoin));

            // Add node to cluster after all nodes accept changes
            final Gossip aNewNodeCompletesToJoin  = new Gossip(node, NodeStatus.STABLE, tokens);
            nodes.values().forEach(n -> n.mergeGossip(aNewNodeCompletesToJoin));
        }
        nodes.put(node.id(), node);
    }

    public void decommission(Node node) {
        if (!nodes.containsKey(node.id())) {
            return;
        }

        List<Node> newTopology = nodes.values().stream()
            .filter(n -> node.id() != n.id())
            .toList();
        TokenMetadata tokenMetadata = node.getTokenMetadata();
        Map<Node, List<Token>> nodeToTokens = tokenMetadata.getNodeToTokens();
        List<Token> heldTokens = nodeToTokens.get(node);
        final Gossip aNodeRequestsToLeave = new Gossip(node, NodeStatus.LEAVING, heldTokens);
        newTopology.forEach(n -> n.mergeGossip(aNodeRequestsToLeave));

        final Gossip aNodeLeaves = new Gossip(node, NodeStatus.REMOVED, heldTokens);
        newTopology.forEach(n -> n.mergeGossip(aNodeLeaves));
        nodes.remove(node.id());
    }

    private List<Token> addTokens(Node node,
                                  NavigableMap<Integer, Token> ring,
                                  Map<Token, Node> tokenToNode,
                                  Map<Node, List<Token>> nodeToTokens
    ) {
        List<Token> tokens = new ArrayList<>(vnodes);
        // Add new virtual nodes
        for (int i = 0; i < vnodes; i++) {
            int partition = HashGenerator.hash(node.id() << 4 + i);
            Token token = new Token(partition, "murmur3");
            ring.put(partition, token);
            tokenToNode.put(token, node);
            tokens.add(token);
        }
        nodeToTokens.put(node, tokens);
        TokenMetadata newTokenMetadata = new TokenMetadata(ring, tokenToNode, nodeToTokens);
        // Renewed metadata applies to a new node only
        node.updateTokenMetadata(newTokenMetadata);

        return tokens;
    }
}
