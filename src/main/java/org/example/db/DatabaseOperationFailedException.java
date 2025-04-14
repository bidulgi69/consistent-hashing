package org.example.db;

public class DatabaseOperationFailedException extends RuntimeException {
    public DatabaseOperationFailedException(String message) {
        super(message);
    }
}
