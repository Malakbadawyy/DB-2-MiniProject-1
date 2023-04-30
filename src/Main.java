import db.DBApp;
import db.DBAppException;
import utils.Utilities;

import javax.swing.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Hashtable;

import static utils.Constants.*;
import static utils.Utilities.inputMessage;
import static utils.Utilities.printMessage;

public class Main {

    private static final ArrayList<String> columnTypes = new ArrayList<>();

    public static void main(String[] args) throws Exception {

//        // ----------------- Welcome Message -----------------
//        welcomeMessage();
//
//        // ----------------- Creating DBApp -----------------
//        String appName = inputMessage("Please enter the name of the application", "Create Application"); // get the name of the application
//        DBApp dbApp = new DBApp(appName); // create the application
//
//        // ----------------- Main Menu -----------------
//        mainMenu(dbApp);

        // ===================================================================================================================================

        int test_cases = 0;
        int passed = 0;
        String Pass = " \u2705";
        String Fail = "\u274C";
        String ANSI_RESET = "\u001B[0m";
        String GREEN_BACKGROUND = "\u001B[32m";
        String RED_BACKGROUND = "\u001B[31m";
        // ----------------- Creating DBApp -----------------
        // TODO: refactor all methods to its responsible class
        DBApp dbApp = new DBApp("Student");

        String strTableName = "Student";

        // ----------------- Creating Table -----------------
        Hashtable htblColNameType = new Hashtable(); // <colName, colType>
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable htblColNameMin = new Hashtable(); // <colName, colMin>
        htblColNameMin.put("id", "0");
        htblColNameMin.put("name", "A");
        htblColNameMin.put("gpa", "0.0");

        Hashtable htblColNameMax = new Hashtable(); // <colName, colMax>
        htblColNameMax.put("id", "990");
        htblColNameMax.put("name", "ZZZZZZZZZZ");
        htblColNameMax.put("gpa", "4.0");

        dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax); // create table

        // ----------------- Inserting Records -----------------
        System.out.println("..............Inserting records..............");
        /**
         * Inserting 500 records into the table
         *  * first 200 records have id from 0 to 199 and gpa = 4.0
         *  * next 200 records have id from 399 to 200 and gpa < 4.0
         */
        Hashtable htblColNameValue = new Hashtable();
        int insertId;
        System.out.println("Inserting first 200 records...");
        // First 200 records
        for (insertId = 0; insertId < 200; insertId++) {
            htblColNameValue.put("id", insertId);
            htblColNameValue.put("name", "Student_" + insertId);
            htblColNameValue.put("gpa", 4.0);
            dbApp.insertIntoTable(strTableName, htblColNameValue); // insert record
            htblColNameValue.clear();
        }
        System.out.println("Inserting second 200 records reverse sorted by id...");
        // Second 200 records id 399 to 200
        for (; insertId < 400; insertId++) {
            htblColNameValue.put("id", 599 - insertId);
            htblColNameValue.put("name", "Student_" + (599 - insertId) );
            htblColNameValue.put("gpa", 3.5);
            dbApp.insertIntoTable(strTableName, htblColNameValue); // insert record
            htblColNameValue.clear();
        }

        System.out.println("Inserting third 100 records...");
        // Third 100 records
        for (; insertId < 500; insertId++) {
            htblColNameValue.put("id", insertId);
            htblColNameValue.put("name", "Student_" + insertId);
            htblColNameValue.put("gpa", 4.0);
            dbApp.insertIntoTable(strTableName, htblColNameValue); // insert record
            htblColNameValue.clear();
        }

        // Trying to insert with no id
        boolean insertionCheck = false;
        try {
            test_cases++;
            System.out.println("Insertion with no key...");
            htblColNameValue.clear();
            htblColNameValue.put("name", "Student_0");
            htblColNameValue.put("gpa", 4.0);
            dbApp.insertIntoTable(strTableName, htblColNameValue); // insert record
        } catch (DBAppException e) {
            insertionCheck = true;
            passed++;
            System.out.println(Pass + GREEN_BACKGROUND + " Insertion with no key failed with error msg: "+ e.getMessage() + ANSI_RESET);
        }
        if (!insertionCheck) 
            System.out.println(Fail + RED_BACKGROUND + " Insertion with no key failed to throw exception." + ANSI_RESET);

        // Trying to insert with different columns
        insertionCheck = false;
        try {
            test_cases++;
            System.out.println("Insertion with wrong columns...");
            htblColNameValue.clear();
            htblColNameValue.put("id", 0);
            htblColNameValue.put("Nationality", "Egyptian");
            htblColNameValue.put("gpa", 4.0);
            dbApp.insertIntoTable(strTableName, htblColNameValue); // insert record
        } catch (DBAppException e) {
            insertionCheck = true;
            passed++;
            System.out.println(Pass + GREEN_BACKGROUND + " Insertion with wrong columns failed with error msg: "+ e.getMessage() + ANSI_RESET);
        }
        if (!insertionCheck) 
            System.out.println(Fail + RED_BACKGROUND + " Insertion with wrong columns failed to throw exception." + ANSI_RESET);

        
        // Trying to insert with duplicate key
        insertionCheck = false;
        try {
            test_cases++;
            System.out.println("Insertion Duplicate key at beginnig of the page...");
            htblColNameValue.clear();
            htblColNameValue.put("id", 0);
            htblColNameValue.put("name", "Student_0");
            htblColNameValue.put("gpa", 4.0);
            dbApp.insertIntoTable(strTableName, htblColNameValue); // insert record
        } catch (DBAppException e) {
            insertionCheck = true;
            passed++;
            System.out.println(Pass + GREEN_BACKGROUND + " Insertion Duplicate key at beginnig of the page failed with error msg: "+ e.getMessage() + ANSI_RESET);
        }
        if (!insertionCheck) 
            System.out.println(Fail + RED_BACKGROUND + " Insertion Duplicate key at beginnig of the page failed to throw exception." + ANSI_RESET);

        insertionCheck = false;
        try {
            test_cases++;
            System.out.println("Insertion Duplicate key at end of the page...");
            htblColNameValue.clear();
            htblColNameValue.put("id", 199);
            htblColNameValue.put("name", "Student_0");
            htblColNameValue.put("gpa", 4.0);
            dbApp.insertIntoTable(strTableName, htblColNameValue); // insert record
        } catch (DBAppException e) {
            insertionCheck = true;
            passed++;
            System.out.println(Pass + GREEN_BACKGROUND + " Insertion Duplicate key at end of the page failed with error msg: "+ e.getMessage() + ANSI_RESET);
        }
        if (!insertionCheck) 
            System.out.println(Fail + RED_BACKGROUND + " Insertion Duplicate key at end of the page failed to throw exception." + ANSI_RESET);


        insertionCheck = false;
        try {
            test_cases++;
            System.out.println("Insertion Duplicate key at middle of the page...");
            htblColNameValue.clear();
            htblColNameValue.put("id", 100);
            htblColNameValue.put("name", "Student_0");
            htblColNameValue.put("gpa", 4.0);
            dbApp.insertIntoTable(strTableName, htblColNameValue); // insert record
        } catch (DBAppException e) {
            insertionCheck = true;
            passed++;
            System.out.println(Pass + GREEN_BACKGROUND + " Insertion Duplicate key at middle of the page failed with error msg: "+ e.getMessage() + ANSI_RESET);
        }
        if (!insertionCheck) 
            System.out.println(Fail + RED_BACKGROUND + " Insertion Duplicate key at middle of the page failed to throw exception." + ANSI_RESET);
        
        // ........................Insertion Done.........................



        // ----------------- Updating Records -----------------
        Hashtable htblColNameValueUpdate = new Hashtable(); // <colName, colValue>
        // First 100 records to another name and gpa 3.0
        Integer updateId;
        for (updateId = 0; updateId < 100; updateId++) {
            htblColNameValueUpdate.put("id", updateId);
            htblColNameValueUpdate.put("name", "Ahmed Mohamed");
            htblColNameValueUpdate.put("gpa", 3.0);
            dbApp.updateTable(strTableName, updateId.toString(), htblColNameValueUpdate); // update record
            htblColNameValueUpdate.clear();
        }
        // trying to update non existing record
        htblColNameValueUpdate.clear();
        htblColNameValueUpdate.put("id", 12333);
        htblColNameValueUpdate.put("name", "Ahmed Mohamed");
        htblColNameValueUpdate.put("gpa", 3.0);
        dbApp.updateTable(strTableName, "12333", htblColNameValueUpdate); // update record

        // Updating record in some of the fields only
        htblColNameValueUpdate.clear();
        htblColNameValueUpdate.put("id", 123);
        htblColNameValueUpdate.put("gpa", 3.0);
        dbApp.updateTable(strTableName, "123", htblColNameValueUpdate); // update record
        // Trying to update primary key
        boolean updateCheck = false;
        try {
            test_cases++;
            System.out.println("Updating primary key...");
            htblColNameValueUpdate.clear();
            htblColNameValueUpdate.put("id", 2);
            htblColNameValueUpdate.put("name", "Ahmed Mohamed");
            htblColNameValueUpdate.put("gpa", 3.0);
            dbApp.updateTable(strTableName, "1", htblColNameValueUpdate); // update record

        } catch (DBAppException e) {
            updateCheck = true;
            passed++;
            System.out.println(Pass + GREEN_BACKGROUND + " Updating primary key failed with error msg: "+ e.getMessage() + ANSI_RESET);
        }
        if (!updateCheck) 
            System.out.println(Fail + RED_BACKGROUND + " Updating primary key failed to throw exception." + ANSI_RESET);

        // ........................Updating Done.........................


        // ----------------- Deleting Records -----------------
        // Delete with primary key
        Hashtable htblColNameValueDelete = new Hashtable(); // <colName, colValue>
        htblColNameValueDelete.put("id", 140);
        dbApp.deleteFromTable(strTableName, htblColNameValueDelete); // delete record

        // Delete with non primary key
        htblColNameValueDelete.clear();
        htblColNameValueDelete.put("gpa", 4.0);
        dbApp.deleteFromTable(strTableName, htblColNameValueDelete); // delete record


        System.out.println("Main program ended successfully.");
        System.out.println("Passed: " + passed + " / " + test_cases + " test cases.");

    }

    private static void mainMenu(DBApp dbApp) throws Exception {
        int option = Utilities.inputOptions("Please select an option", "Main Menu", new String[]{"Create Table", "Insert Record", "Update Record", "Delete Record", "Exit"});
        switch (option) {
            case 0 -> createTable(dbApp);
            case 1 -> insertRecord(dbApp);
            case 2 -> updateRecord(dbApp);
            case 3 -> deleteRecord(dbApp);
            case 4 -> System.exit(0); // 0 means no error
        }
    }

    private static void createTable(DBApp dbApp) throws Exception {
        // ----------------- Creating Table -----------------
        String tableName = inputMessage("Please enter the name of the table", "Create Table"); // get the name of the table
        String primaryKey = inputMessage("Please enter the name of the primary key", "Create Table"); // get the name of the primary key

        Hashtable<String, String> htblColNameType = new Hashtable<>(); // <colName, colType>
        String[] columnNames = inputMessage("Please enter the names of the columns separated by a comma {id, name, gpa}", "Create Table").split(","); // get the names of the columns
        for (String columnName : columnNames) { // get the types of the columns from the user
            int option = Utilities.inputOptions("Please select the type of the column " + columnName, "Create Table", new String[]{"Integer", "String", "Double", "Date"});
            switch (option) {
                case 0 -> htblColNameType.put(columnName, INT_TYPE);
                case 1 -> htblColNameType.put(columnName, STRING_TYPE);
                case 2 -> htblColNameType.put(columnName, DOUBLE_TYPE);
                case 3 -> htblColNameType.put(columnName, DATE_TYPE);
            }
            columnTypes.add(htblColNameType.get(columnName)); // add the type of the column to the columnTypes list
        }

        Hashtable<String, String> htblColNameMin = new Hashtable<>(); // <colName, colMin>
        String[] columnMin = inputMessage("Please enter the minimum values of the columns separated by a comma {0, A, 0.0}", "Create Table").split(","); // get the minimum values of the columns
        for (int i = 0; i < columnMin.length; i++) {
            htblColNameMin.put(columnNames[i], columnMin[i]);
        }

        Hashtable<String, String> htblColNameMax = new Hashtable<>(); // <colName, colMax>
        String[] columnMax = inputMessage("Please enter the maximum values of the columns separated by a comma {100, Z, 4.0}", "Create Table").split(","); // get the maximum values of the columns
        for (int i = 0; i < columnMax.length; i++) {
            htblColNameMax.put(columnNames[i], columnMax[i]);
        }

        dbApp.createTable(tableName, primaryKey, htblColNameType, htblColNameMin, htblColNameMax); // create the table
        printMessage("Table " + "\"" + tableName + "\"" + " has been created successfully", "Create Table", JOptionPane.INFORMATION_MESSAGE);

        mainMenu(dbApp);
    }

    private static void insertRecord(DBApp dbApp) throws Exception {
        // ----------------- Inserting Records -----------------
        int numberOfRecords = Integer.parseInt(inputMessage("Please enter the number of records you want to insert", "Insert Record")); // get the number of records to insert
        for (int i = 0; i < numberOfRecords; i++) {
            String tableName = inputMessage("Please enter the name of the table", "Insert Record"); // get the name of the table
            String[] columnNames = inputMessage("Please enter the names of the columns separated by a comma {id, name, gpa}", "Insert Record").split(","); // get the names of the columns
            String[] columnValues = inputMessage("Please enter the values of the columns separated by a comma {1, Ahmed, 3.5}", "Insert Record").split(","); // get the values of the columns
            Hashtable<String, Object> htblColNameValue = new Hashtable<>(); // <colName, colValue>
            for (int j = 0; j < columnValues.length; j++) {
                switch (columnTypes.get(j)) {
                    case INT_TYPE -> htblColNameValue.put(columnNames[j], Integer.parseInt(columnValues[j]));
                    case STRING_TYPE -> htblColNameValue.put(columnNames[j], columnValues[j]);
                    case DOUBLE_TYPE -> htblColNameValue.put(columnNames[j], Double.parseDouble(columnValues[j]));
                    case DATE_TYPE ->
                            htblColNameValue.put(columnNames[j], new SimpleDateFormat("yyyy-MM-dd").parse(columnValues[j]));
                }
            }
            dbApp.insertIntoTable(tableName, htblColNameValue); // insert the record
            printMessage("Record " + (i + 1) + " has been inserted successfully", "Insert Record", JOptionPane.INFORMATION_MESSAGE);
        }

        mainMenu(dbApp);
    }

    private static void updateRecord(DBApp dbApp) throws Exception {
        // ----------------- Updating Records -----------------
        String tableName = inputMessage("Please enter the name of the table", "Update Record"); // get the name of the table
        String key = inputMessage("Please enter the value of the primary key", "Update Record"); // get the value of the primary key
        String[] columnNames = inputMessage("Please enter the names of the columns separated by a comma {id, name, gpa}", "Update Record").split(","); // get the names of the columns
        String[] columnValues = inputMessage("Please enter the values of the columns separated by a comma {1, Ahmed, 3.5}", "Update Record").split(","); // get the values of the columns
        Hashtable<String, Object> htblColNameValue = new Hashtable<>(); // <colName, colValue>
        for (int i = 0; i < columnValues.length; i++) {
            switch (columnTypes.get(i)) {
                case INT_TYPE -> htblColNameValue.put(columnNames[i], Integer.parseInt(columnValues[i]));
                case STRING_TYPE -> htblColNameValue.put(columnNames[i], columnValues[i]);
                case DOUBLE_TYPE -> htblColNameValue.put(columnNames[i], Double.parseDouble(columnValues[i]));
                case DATE_TYPE ->
                        htblColNameValue.put(columnNames[i], new SimpleDateFormat("yyyy-MM-dd").parse(columnValues[i]));
            }
        }
        dbApp.updateTable(tableName, key, htblColNameValue); // update the record
        printMessage("Record has been updated successfully", "Update Record", JOptionPane.INFORMATION_MESSAGE);

        mainMenu(dbApp);
    }

    private static void deleteRecord(DBApp dbApp) throws Exception {
        // ----------------- Deleting Records -----------------
        String tableName = inputMessage("Please enter the name of the table", "Delete Record"); // get the name of the table
        String primaryKey = inputMessage("Please enter the name of the primary key", "Delete Record"); // get the name of the primary key
        String key = inputMessage("Please enter the value of the primary key", "Delete Record"); // get the value of the primary key
        Hashtable<String, Object> htblColNameValue = new Hashtable<>(); // <colName, colValue>
        htblColNameValue.put(primaryKey, Integer.parseInt(key));
        dbApp.deleteFromTable(tableName, htblColNameValue); // delete the record
        printMessage("Record has been deleted successfully", "Delete Record", JOptionPane.INFORMATION_MESSAGE);

        mainMenu(dbApp);
    }
}