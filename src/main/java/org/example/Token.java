package org.example;

public record Token(
    int partition,
    String algorithm
) implements Comparable<Token> {


    @Override
    public int compareTo(Token o) {
        return Integer.compare(partition, o.partition());
    }
}
