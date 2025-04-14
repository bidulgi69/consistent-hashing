package org.example;

import java.util.*;

public class TokenMetadata {

    private final NavigableMap<Integer, Token> ring;
    private final Map<Token, Node> tokenToNode;
    private final Map<Node, List<Token>> nodeToTokens;

    public TokenMetadata(NavigableMap<Integer, Token> ring,
                         Map<Token, Node> tokenToNode,
                         Map<Node, List<Token>> nodeToTokens
    ) {
        this.ring = ring;
        this.tokenToNode = tokenToNode;
        this.nodeToTokens = nodeToTokens;
    }

    public Node getNode(int partition) {
        int hash = partition;
        if (!ring.containsKey(hash)) {
            SortedMap<Integer, Token> tail = ring.tailMap(hash);
            hash = tail.isEmpty() ? ring.firstKey() : tail.firstKey();
        }
        Token token = ring.get(hash);
        return tokenToNode.get(token);
    }

    public void updateNormalToken(Node newNode, List<Token> tokens) {
        tokens.forEach(token -> {
            ring.put(token.partition(), token);
            tokenToNode.put(token, newNode);
        });
        nodeToTokens.put(newNode, tokens);
    }

    public void removeEndpoint(Node node) {
        List<Token> tokens = nodeToTokens.get(node);
        tokens.forEach(token -> {
            ring.remove(token.partition());
            tokenToNode.remove(token);
        });
        nodeToTokens.remove(node);
    }

    // deep copy
    public NavigableMap<Integer, Token> getRing() {
        return new TreeMap<>(ring);
    }

    public Map<Token, Node> getTokenToNode() {
        return new HashMap<>(tokenToNode);
    }

    public Map<Node, List<Token>> getNodeToTokens() {
        return new HashMap<>(nodeToTokens);
    }
}
