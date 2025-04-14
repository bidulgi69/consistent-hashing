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
                // send data streams to new node
                Collections.sort(gossip.tokens());

                NavigableMap<Integer, Token> ring = tokenMetadata.getRing();
                for (Token token : gossip.tokens()) {
                    int tailKey = ring.tailMap(token.partition()).firstKey();
                    Node responsibleNode = tokenMetadata.getNode(tailKey);
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
                }
            }
            case STABLE -> {
                // update token metadata
                tokenMetadata.updateNormalToken(gossip.node(), gossip.tokens());
            }
            case LEAVING -> {
                // receive data stream from leaving node
                // rebalance tokens of leaving node
                NavigableMap<Integer, Token> ring = tokenMetadata.getRing();
                for (Token token : gossip.tokens()) {
                    int tailKey = ring.tailMap(token.partition()).firstKey();
                    Node responsibleNode = tokenMetadata.getNode(tailKey);
                    DbAccess.ScanAccess scanAccess = new DbAccess.ScanAccess(token.partition(), tailKey);
                    Result result = gossip.node().process(scanAccess);

                    if (result instanceof Result.Error) {
                        throw new DatabaseOperationFailedException("Error while scanning node " + gossip.node().nodeName);
                    }

                    // rebalance
                    for (Map.Entry<Integer, Entity> entry : ((Result.Ok<Set<Map.Entry<Integer, Entity>>>) result).value()) {
                        Entity entity = entry.getValue();
                        DbAccess.PutAccess putAccess = new DbAccess.PutAccess(entity.id(), entity.value());
                        responsibleNode.process(putAccess);
                    }

                    ring.put(token.partition(), token);
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
}
