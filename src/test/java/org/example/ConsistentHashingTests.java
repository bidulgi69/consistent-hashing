package org.example;

import org.example.db.DbAccess;
import org.example.db.Entity;
import org.example.db.Snowflake;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

public class ConsistentHashingTests {

    @Test
    void test_rebalance_on_node_joins() throws NoSuchAlgorithmException {
        int vnodes = 1024;
        Topology topology = new Topology(vnodes);
        String nodeId1 = "node-1";
        Node n1 = new Node(HashGenerator.generateNodeId(nodeId1), nodeId1);
        topology.join(n1);

        TokenMetadata tokenMetadata = n1.getTokenMetadata();
        Map<Long, String> idToNode = new HashMap<>();
        // generate ids
        for (int i = 0; i < vnodes; i++) {
            long snowflakeId = Snowflake.getInstance().nextId();
            long id = snowflakeId << 2 + i;
            Token vnode = n1.getToken(HashGenerator.hash(id), tokenMetadata.getRing());
            Node responsibleNode = tokenMetadata.getTokenToNode().get(vnode);

            idToNode.put(id, responsibleNode.nodeName());
        }

        // a new node joins
        String nodeId2 = "node-2";
        Node n2 = new Node(HashGenerator.generateNodeId(nodeId2), nodeId2);
        topology.join(n2);

        // check distribution of ids
        int moved = 0;
        for (Map.Entry<Long, String> entry : idToNode.entrySet()) {
            Token vnode = n1.getToken(HashGenerator.hash(entry.getKey()), tokenMetadata.getRing());
            Node node = tokenMetadata.getTokenToNode().get(vnode);
            if (!node.nodeName().equals(entry.getValue())) {
                moved++;
            }
        }
        double pct = (moved / (double) vnodes) * 100;
        Assertions.assertTrue(pct < 50); // expected: 1/N(num of nodes)≈ 50


        // a new node joins
        String nodeId3 = "node-3";
        Node n3 = new Node(HashGenerator.generateNodeId(nodeId3), nodeId3);
        topology.join(n3);

        idToNode.clear();
        // generate ids
        for (int i = 0; i < vnodes; i++) {
            long snowflakeId = Snowflake.getInstance().nextId();
            long id = snowflakeId << 2 + i;
            Token vnode = n1.getToken(HashGenerator.hash(id), tokenMetadata.getRing());
            Node responsibleNode = tokenMetadata.getTokenToNode().get(vnode);

            idToNode.put(id, responsibleNode.nodeName());
        }

        moved = 0;
        for (Map.Entry<Long, String> entry : idToNode.entrySet()) {
            Token vnode = n1.getToken(HashGenerator.hash(entry.getKey()), tokenMetadata.getRing());
            Node node = tokenMetadata.getTokenToNode().get(vnode);
            if (!node.nodeName().equals(entry.getValue())) {
                moved++;
            }
        }

        pct = (moved / (double) vnodes) * 100;
        Assertions.assertTrue(pct < 33); // expected: 1/N(num of nodes)≈ 33
    }

    @Test
    void test_rebalance_on_node_leaves() throws NoSuchAlgorithmException {
        int vnodes = 512;
        Topology topology = new Topology(vnodes);
        String nodeId1 = "node-1";
        Node n1 = new Node(HashGenerator.generateNodeId(nodeId1), nodeId1);
        topology.join(n1);

        String nodeId2 = "node-2";
        Node n2 = new Node(HashGenerator.generateNodeId(nodeId2), nodeId2);
        topology.join(n2);

        TokenMetadata tokenMetadata = n1.getTokenMetadata();
        // insert
        Map<Long, String> idToNode = new HashMap<>();
        for (int i = 0; i < vnodes; i++) {
            long snowflakeId = Snowflake.getInstance().nextId();
            long id = snowflakeId << 4 + i;
            DbAccess.PutAccess putAccess = new DbAccess.PutAccess(id, "v");
            Token vnode = n1.getToken(HashGenerator.hash(id), tokenMetadata.getRing());
            Node responsibleNode = tokenMetadata.getTokenToNode().get(vnode);

            responsibleNode.process(putAccess);
            idToNode.put(id, responsibleNode.nodeName());
        }

        // a node requests to leave
        topology.decommission(n1);

        boolean rebalanced = idToNode.entrySet().stream()
            .filter(entry -> entry.getValue().equals(n1.nodeName()))
            .allMatch(entry -> {
                DbAccess.GetAccess getAccess = new DbAccess.GetAccess(entry.getKey());
                Result result = n2.process(getAccess);

                if (result instanceof Result.Error) {
                    return false;
                }

                @SuppressWarnings("unchecked")
                Entity entity = ((Result.Ok<Entity>) result).value();
                // data moved to node-2
                return entity != null;
            });

        Assertions.assertTrue(rebalanced);
    }
}
