package utils;

import models.Table;

import javax.swing.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class Utilities {
    private Utilities() {
        // private constructor to prevent instantiation of this class
    }

    public static Table readTableFromFile(String directoryPath, String strTableName) throws IOException, ClassNotFoundException {
        File tableFile = new File(directoryPath + strTableName + "/" + strTableName + ".class"); // table file path
        ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(tableFile)); // read table from file using objectInputStream
        Table table = (Table) objectInputStream.readObject(); // read the table object from the file and cast it to Table class
        objectInputStream.close(); // close the objectInputStream to prevent memory leaks
        return table; // return the table object
    }

    // ----------------- GUI Methods -----------------
    public static void welcomeMessage() {
        JOptionPane.showMessageDialog(null, "Welcome to the Database II-MET Project 1", "Welcome", JOptionPane.INFORMATION_MESSAGE);
    }

    public static void printMessage(String message, String title, int messageType) {
        JOptionPane.showMessageDialog(new JButton(), message, title, messageType);
    }

    public static String inputMessage(String message, String title) {
        return JOptionPane.showInputDialog(new JButton(), message, title, JOptionPane.QUESTION_MESSAGE);
    }

    public static int inputOptions(String message, String title, String[] options) {
        return JOptionPane.showOptionDialog(new JButton(), message, title, JOptionPane.DEFAULT_OPTION, JOptionPane.QUESTION_MESSAGE, null, options, options[0]);
    }
}
