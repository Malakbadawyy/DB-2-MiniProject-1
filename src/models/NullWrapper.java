package models;

public class NullWrapper implements Comparable<Object> {
    @Override
    public boolean equals(Object o) {
        return false;
    }

    @Override
    public int compareTo(Object o) {
        return -1;
    }

    @Override
    public String toString() {
        return "null";
    }
}