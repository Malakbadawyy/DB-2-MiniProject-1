package db;

import models.Page;
import models.Table;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import static utils.Constants.*;
import static utils.Utilities.readTableFromFile;

public class DBApp {
    private static String directoryPath;
    private boolean result;
    private String logMessage;

    public DBApp(String appName) { // DBApp constructor (initialization)
        try{
            Properties properties = new Properties(); // create a properties object
            properties.load(new FileInputStream(CONFIG_PATH));// read the properties file
            Page.MAX_TUPLES = Integer.parseInt(properties.getProperty("MaximumRowsCountinTablePage")); // set the maximum number of tuples (rows) in a page    
        }
        catch (Exception e)
        {
            Page.MAX_TUPLES = DEFAULT_MAXIMUM_ROWS_COUNT_IN_PAGE; // set the maximum number of tuples (rows) in a page
        }
        // create the app directory inside the data directory
        directoryPath = DATA_DIRECTORY_PATH + appName + "/"; // initialize the directory path of the app
        result = new File(directoryPath).mkdirs(); // create the directory if it doesn't exist
        // logMessage = (result) ? "Directory " + directoryPath + " created successfully" : "Directory " + directoryPath + " already exists";
        // System.out.println(logMessage);

        // create the metadata directory inside the data directory
        String metaPath = directoryPath + "Meta/"; // initialize the directory path of the app metadata files (csv files)
        result = new File(metaPath).mkdirs(); // create the directory if it doesn't exist
        // logMessage = (result) ? "Directory " + metaPath + " created successfully" : "Directory " + metaPath + " already exists";
        // System.out.println(logMessage);

    }

    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String,String> htblColNameType,
                            Hashtable<String, String> htblColNameMin,
                            Hashtable<String,String> htblColNameMax)
                            throws DBAppException{
        /*
        The following method creates one table only.
        strClusteringKeyColumn is the name of the column that will be the primary key and the clustering column as well.
        The data type of that column will be passed in htblColNameType.
        htblColNameValue will have the column name as key and the data type as value.
        htblColNameMin and htblColNameMax are for passing minimum and maximum values for data in the column.
        Key is the name of the column.
         */
        try{
            // Check if the table already exists
            if (Table.tableExists(DBApp.directoryPath, strTableName))
                throw new DBAppException("Table already exists");

            // create the table object and save it to a file in the data directory
            new Table(directoryPath, strTableName, htblColNameType, htblColNameMin, htblColNameMax, strClusteringKeyColumn);

            // create the metadata file for the table
            createTableMetadata(strTableName, htblColNameType, strClusteringKeyColumn, htblColNameMin, htblColNameMax);
        } 
        catch (Exception e) {
            throw new DBAppException("Error in app: " + e.getMessage());
        }
    }

    private void createTableMetadata(String strTableName, Hashtable<String, String> htblColNameType, String strClusteringKeyColumn,
                                     Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws IOException {
        File metadataFile = new File(directoryPath + "Meta" + "/" + "metadata.csv"); // create the metadata file object
        result = metadataFile.createNewFile(); // create the file if it doesn't exist
        // logMessage = (result) ? "File " + metadataFile.getName() + " created successfully" : "File " + metadataFile.getName() + " already exists";
        // System.out.println(logMessage);

        PrintWriter writer = new PrintWriter(new FileWriter(metadataFile, true)); // create the writer object to write to the file in append mode
        writer.println(" Page maximum row count is, " + Page.MAX_TUPLES); // write the maximum number of rows in a page
        writer.println("Table Name,Column Name,Column Type,ClusteringKey,Min,Max"); // write the header (column titles)
        for (Map.Entry<String, String> entry : htblColNameType.entrySet()) { // write the data (column names)
            writer.println(strTableName + "," + entry.getKey() + "," + entry.getValue() + "," + (entry.getKey().equals(strClusteringKeyColumn) ? "True" : "False") + "," + htblColNameMin.get(entry.getKey()) + "," + htblColNameMax.get(entry.getKey()));
        writer.flush(); // flush the writer to empty the buffer
    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue)
                                throws DBAppException{
        /*
        The following method inserts one row only into the table.
        htblColNameValue contains the column name as key and the value as value.
         */
        try{
            // Check if the table exists
            if (!Table.tableExists(DBApp.directoryPath, strTableName))
                throw new DBAppException("Table doesn't exist");

            // Insert the row
            Table table = readTableFromFile(directoryPath, strTableName);
            table.insertTuple(htblColNameValue);
            table.saveTableToFile();
        }
        catch (Exception e) {
            throw new DBAppException("Error in app: " + e.getMessage());
        }
    }

    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue)
                            throws DBAppException {
        /*
        The following method updates one row only in the table.
        htblColNameValue holds the key and new value.
        htblColNameValue will not include clustering key as column name.
        strClusteringKeyValue is the value to look for to find the row to update.
         */
        try{
            // Check if the table exists
            if (!Table.tableExists(DBApp.directoryPath, strTableName))
                throw new DBAppException("Table doesn't exist");

            // Update the row
            Table table = readTableFromFile(directoryPath, strTableName); // read the table from the file
            String primaryType = table.getPrimaryType(); // get the primary type of the table
            Object objectKey = switch (primaryType) { // convert the primary key to the correct type
                case INT_TYPE -> Integer.parseInt(strClusteringKeyValue);
                case DOUBLE_TYPE -> Double.parseDouble(strClusteringKeyValue);
                case BOOLEAN_TYPE -> Boolean.parseBoolean(strClusteringKeyValue);
                case DATE_TYPE -> Date.parse(strClusteringKeyValue);
                default -> strClusteringKeyValue;
            };

            table.updateTuple(objectKey, htblColNameValue); // update the tuple
            table.saveTableToFile(); // save the table
        }
        catch (Exception e) {
            throw new DBAppException("Error in app: " + e.getMessage());
        }
    }

    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue)
                                throws DBAppException {
        /*
        The following method deletes one or more rows from the table.
        htblColNameValue holds the key and value.
        This will be used in search to identify which rows/tuples to delete.
        htblColNameValue enteries are ANDED together.
         */
        try{
            // Check if the table exists
            if (!Table.tableExists(DBApp.directoryPath, strTableName))
                throw new DBAppException("Table doesn't exist");

            // Delete the rows
            Table table = readTableFromFile(directoryPath, strTableName); // read the table from the file
            table.deleteTuple(htblColNameValue); // delete the tuple
            table.saveTableToFile(); // save the table
        }
        catch (Exception e) {
            throw new DBAppException("Error in app: " + e.getMessage());
        }
    }
}
