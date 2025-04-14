package org.example;

import org.example.db.DatabaseOperationFailedException;
import org.example.db.DbAccess;
import org.example.db.Entity;
import org.example.db.LocalFileSystem;

import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

public class Node {

    private final long id;
    private final String nodeName;
    private final LocalFileSystem database;
    private TokenMetadata tokenMetadata;

    public Node(long id, String nodeName) {
        this.id = id;
        this.nodeName = nodeName;
        this.database = new LocalFileSystem();
    }

    public long id() {
        return id;
    }

    public String nodeName() {
        return nodeName;
    }

    public void updateTokenMetadata(TokenMetadata tokenMetadata) {
        this.tokenMetadata = tokenMetadata;
    }

    @SuppressWarnings("unchecked")
    public void mergeGossip(Gossip gossip) {
        switch (gossip.status()) {
            case BOOTSTRAPING -> {
                Node newNode = gossip.node();
                Collections.sort(gossip.tokens());

                NavigableMap<Integer, Token> ring = tokenMetadata.getRing();
                Map<Token, Node> tokenToNode = tokenMetadata.getTokenToNode();
                // rebalance tokens
                for (Token token : gossip.tokens()) {
                    Integer tailKey = ring.ceilingKey(token.partition());
                    if (tailKey == null) {
                        tailKey = ring.firstKey();
                    }

                    Token vnode = getToken(tailKey, ring);
                    Node responsibleNode = tokenToNode.get(vnode);
                    DbAccess.ScanAccess scanAccess = new DbAccess.ScanAccess(token.partition(), tailKey);
                    Result result = responsibleNode.process(scanAccess);

                    if (result instanceof Result.Error) {
                        throw new DatabaseOperationFailedException("Error while scanning node " + responsibleNode.nodeName);
                    }

                    // takeover
                    for (Map.Entry<Integer, Entity> entry : ((Result.Ok<Set<Map.Entry<Integer, Entity>>>) result).value()) {
                        Entity entity = entry.getValue();
                        DbAccess.PutAccess putAccess = new DbAccess.PutAccess(entity.id(), entity.value());
                        newNode.process(putAccess);
                    }

                    ring.put(token.partition(), token);
                    tokenToNode.put(token, gossip.node());
                }
            }
            case STABLE -> {
                // update token metadata
                tokenMetadata.updateNormalToken(gossip.node(), gossip.tokens());
            }
            case LEAVING -> {
                // receive data stream from leaving node
                // rebalance tokens in leaving node
                Collections.sort(gossip.tokens());
                NavigableMap<Integer, Token> ring = tokenMetadata.getRing();
                Map<Token, Node> tokenToNode = tokenMetadata.getTokenToNode();

                for (Token token : gossip.tokens()) {
                    ring.remove(token.partition());
                    Integer headKey = ring.lowerKey(token.partition());
                    if (headKey == null) {
                        headKey = ring.lastKey();
                    }

                    Token vnode = getToken(token.partition(), ring);
                    Node responsibleNode = tokenToNode.get(vnode);

                    if (gossip.node().id() == responsibleNode.id()) {
                        continue;
                    }

                    // get (prevToken, T]
                    DbAccess.ScanAccess scanAccess = new DbAccess.ScanAccess(headKey, token.partition());
                    Result result = gossip.node().process(scanAccess);

                    if (result instanceof Result.Error) {
                        throw new DatabaseOperationFailedException("Error while scanning node " + gossip.node().nodeName);
                    }

                    // handoff
                    for (Map.Entry<Integer, Entity> entry : ((Result.Ok<Set<Map.Entry<Integer, Entity>>>) result).value()) {
                        Entity entity = entry.getValue();
                        DbAccess.PutAccess putAccess = new DbAccess.PutAccess(entity.id(), entity.value());
                        responsibleNode.process(putAccess);
                    }

                    tokenToNode.remove(token);
                }
            }
            case REMOVED -> {
                // update(delete) token metadata
                tokenMetadata.removeEndpoint(gossip.node());
            }
        }
    }

    public TokenMetadata getTokenMetadata() {
        return tokenMetadata;
    }

    public Result process(DbAccess access) {
        switch (access.getMethod()) {
            case PUT: {
                DbAccess.PutAccess putAccess = (DbAccess.PutAccess) access;
                Entity entity = new Entity(putAccess.getKey(), putAccess.getValue());
                database.write(HashGenerator.hash(entity.id()), entity);
                return new Result.Ok<>(null);
            }
            case GET: {
                DbAccess.GetAccess getAccess = (DbAccess.GetAccess) access;
                int partition = HashGenerator.hash(getAccess.getKey());
                return new Result.Ok<>(database.read(partition));
            }
            case DELETE: {
                DbAccess.DeleteAccess deleteAccess = (DbAccess.DeleteAccess) access;
                int partition = HashGenerator.hash(deleteAccess.getKey());
                database.delete(partition);
                return new Result.Ok<>(null);
            }
            case SCAN: {
                DbAccess.ScanAccess scanAccess = (DbAccess.ScanAccess) access;
                return new Result.Ok<>(database.scan(scanAccess.getFromPartition(), scanAccess.getToPartition()));
            }
        }

        return new Result.Error(new IllegalStateException("Unknown access method: " + access.getMethod()));
    }

    // clock-wise ownership
    // [T, nextToken)
    public Token getToken(int partition, NavigableMap<Integer, Token> ring) {
        Integer tailKey = ring.ceilingKey(partition);
        if (tailKey == null) {
            tailKey = ring.firstKey(); // wrap-around
        }

        return ring.get(tailKey);
    }
}
