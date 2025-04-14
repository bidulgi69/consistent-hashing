package org.example.db;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class LocalFileSystem {

    // partition key : entity
    private final SortedMap<Integer, Entity> filesystem = new TreeMap<>();

    public void write(int key, Entity value) {
        filesystem.put(key, value);
    }

    public Entity read(int key) {
        return filesystem.get(key);
    }

    public void delete(int key) {
        filesystem.remove(key);
    }

    public Set<Map.Entry<Integer, Entity>> scan(int fromKey, int toKey) {
        SortedMap<Integer, Entity> subMap = filesystem.subMap(fromKey, toKey);
        return subMap.entrySet();
    }
}
