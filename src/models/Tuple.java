package models;

import java.io.Serializable;
import java.util.Vector;

public class Tuple implements Serializable, Comparable<Tuple> { // Serializable to be able to write the object to a file
    private final Vector<Object> values; // values of the tuple
    private final Object primaryKey; // primary key of the tuple

    public Tuple(int size, Object primaryKey) {
        this.values = new Vector<>(size); // initialize the values array
        this.primaryKey = primaryKey; // initialize the primary key of the tuple
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Object value : values) {
            stringBuilder.append(value).append(","); // append the value to the string builder
        }
        return stringBuilder.toString();
    }

    public void insertValue(int index, Object value) {
        this.values.add(index, value); // insert the value at the given index
    }

    public Object getValue(int index) {
        return this.values.get(index); // return the value at the given index
    }

    public Object getPrimaryKey() {
        return this.primaryKey; // return the primary key of the tuple
    }

    @Override
    public int compareTo(Tuple other) {
        return ((Comparable)this.primaryKey).compareTo(((Comparable) other.primaryKey)); // compare the primary keys of the tuples
    }

}
