package models;

import db.DBAppException;

import java.io.*;
import java.util.Collections;
import java.util.Vector;

public class Page implements Serializable { // Serializable is used to write objects to file (serialization)

    public static int MAX_TUPLES; // Maximum number of tuples in a page
    private final Vector<Tuple> tuples; // Array of tuples in the page
    private int numTuples; // Number of tuples in the page
    //EDIT: path is not final (in case delete if page is empty make next pages' path shift one)
    private String path; // Path of the page file

    public Page(String path) throws IOException {
        this.tuples = new Vector<>(MAX_TUPLES); // Initialize the array of tuples with the maximum number of tuples
        this.numTuples = 0; // Initialize the number of tuples to 0
        this.path = path; // Initialize the path of the page
        writePage(); // Write the page to the file
    }

    public void setPath(String path) throws IOException {
        File file = new File(this.path);
        file.delete();
        this.path = path;
        writePage();
    }

    public Object[] insertTuple(Tuple tuple) throws IOException, DBAppException { // Used to add a tuple to the page
        if (isFull()) {// Check if the page is full
            throw new DBAppException("Page is full");
        } else { // Check if the page has capacity for the tuple

            int insertionPoint = Collections.binarySearch(tuples, tuple); // Binary search to find the index of the tuple
            if (insertionPoint >= 0) {
                throw new DBAppException("Tuple already exists");
            } else {
                insertionPoint = -(insertionPoint + 1);
            }

            tuples.insertElementAt(tuple, insertionPoint); // Add the tuple to the page
            numTuples = tuples.size(); // Increment the number of tuples
            writePage(); // Write the page to the file
        
            // return the first and last keys of the page
            return new Object[]{tuples.get(0).getPrimaryKey(), tuples.get(tuples.size() - 1).getPrimaryKey()};
        }
    }


    public void writePage() throws IOException { // Used to write the page to the file (serialization)

        boolean result = false;
        String logMessage;

        File file = new File(this.path); // Create a file object with the path of the page
        if (file.exists()) // Check if the file exists
            result = file.delete(); // Delete the file
        // logMessage = (result) ? "File " + file.getName() + " deleted successfully" : "File " + file.getName() + " deletion failed";
        // System.out.println(logMessage);

        result = file.createNewFile(); // Create a new file
        // logMessage = (result) ? "File " + file.getName() + " created successfully" : "File " + file.getName() + " creation failed";
        // System.out.println(logMessage);

        ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(file)); // Create an output stream to write the page to the file
        outputStream.writeObject(this); // Write the page to the file
        outputStream.close(); // Close the output stream to free resources and flush the stream
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < MAX_TUPLES; i++) {
            if (i < numTuples)
                sb.append(tuples.get(i).toString()).append("\n");
            else
                sb.append("====================================\n");
        }
        return sb.toString();
    }

    public boolean isFull() { // Used to check if the page is full
        return this.numTuples == MAX_TUPLES;
    }

    public boolean isEmpty() { // Used to check if the page is empty
        return this.numTuples == 0;
    }

    public Object[] deleteByIndex(int Index) throws IOException { // Used to decrement the number of tuples in the page
        tuples.removeElementAt(Index);
        numTuples = tuples.size();

        if (numTuples == 0) { // If the page is empty, delete the file
            File file = new File(this.path);
            file.delete();
            return new Object[]{-1, -1};
        }
        else
            writePage();
        return new Object[]{tuples.get(0).getPrimaryKey(), tuples.get(tuples.size() - 1).getPrimaryKey()};
    }

    public int getSize() { // Used to get the size of the page (number of tuples)
        return this.numTuples;
    }

    public Vector<Tuple> getTuples() { // Used to get the tuples in the page
        return tuples;
    }

}