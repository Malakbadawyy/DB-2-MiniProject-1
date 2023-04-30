package db;

public class DBAppException extends Exception { // TODO: make this class extend Exception and other classes extend it
    public DBAppException(String message) { // constructor to allow for instantiation with a message
        super(message);
    }
}
