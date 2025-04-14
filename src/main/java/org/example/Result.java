package org.example;

public sealed abstract class Result {

    public static final class Ok<T> extends Result {
        private final T value;

        public Ok(T value) {
            this.value = value;
        }

        public T value() {
            return value;
        }
    }

    public static final class Error extends Result {
        private final Throwable error;

        public Error(Throwable error) {
            this.error = error;
        }

        public Throwable error() {
            return error;
        }
    }
}
