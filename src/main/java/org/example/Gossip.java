package org.example;

import java.util.List;

public record Gossip(
    Node node,
    NodeStatus status,
    List<Token> tokens
) {
}
