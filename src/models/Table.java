package models;

import db.DBAppException;

import java.io.*;
import java.util.*;

import static utils.Constants.*;

public class Table implements Serializable { // Serializable is used to write objects to file (serialization)
    private int lastPage; // last page number (file number)
    private final Vector<Object> minPrimaryKeyOfEachPage; // vector of first key value in each pages (it is string since we always use strings in comparison) --CHECK: Piaza question @132
    private final Vector<Object> maxPrimaryKeyOfEachPage; // vector of last key value in each pages (it is string since we always use strings in comparison) --CHECK: Piaza question @132
    private final String directoryPath; // path of the table
    private final String tableName; // table name (file name)
    private final Hashtable<String, String> htblColNameType; // column name and type
    private final Hashtable<String, String> htblColNameMin; // for passing minimum values for data in the column. Key is the name of the column.
    private final Hashtable<String, String> htblColNameMax; // for passing maximum values for data in the column. Key is the name of the column.
    private final String primaryKey; // primary key of the table

    public Table(String directoryPath, String tableName, Hashtable<String, String> htblColNameType, Hashtable<String,
            String> htblColNameMin, Hashtable<String, String> htblColNameMax, String primaryKey) throws DBAppException, IOException {

        if (!htblColNameType.containsKey(primaryKey)) // check if primary key is an attribute
            throw new DBAppException("primary key must be in the columns");

        this.lastPage = -1; // initialize last page to -1 (no pages)
        this.minPrimaryKeyOfEachPage = new Vector<>(); // initialize vector of first key value in each pages
        this.maxPrimaryKeyOfEachPage = new Vector<>(); // initialize vector of last key value in each pages
        this.directoryPath = directoryPath + tableName + "/"; // initialize path of the table (./data/appName/tableName/)
        this.tableName = tableName; // initialize table name
        this.htblColNameType = htblColNameType; // initialize column name and type (data types)
        this.htblColNameMin = htblColNameMin; // initialize column name and type (minimum values)
        this.htblColNameMax = htblColNameMax; // initialize column name and type (maximum values)
        this.primaryKey = primaryKey; // initialize primary key

        createTableDirectory(); // create directory for the table
        saveTableToFile(); // save table in file
    }

    private boolean result;
    private String logMessage;

    private void createTableDirectory() {
        File tableDir = new File(this.directoryPath); // create file object with the path of the table
        result = tableDir.mkdirs(); // create directory for the table
        // logMessage = (result) ? "Table directory " + this.directoryPath + " created successfully" : "Table directory " + this.directoryPath + " already exists";
        // System.out.println(logMessage);
    }

    public void saveTableToFile() throws IOException {
        File file = new File(directoryPath + tableName + ".class"); // create file object with the path of the table
        if (file.exists()) // check if the file exists
            result = file.delete(); // delete the file
        // logMessage = (result) ? "Table file " + tableName + ".class deleted successfully" : "Table file " + tableName + ".class does not exist";
        // System.out.println(logMessage);

        result = file.createNewFile(); // create new file
        // logMessage = (result) ? "Table file " + tableName + ".class created successfully" : "Table file " + tableName + ".class already exists";
        // System.out.println(logMessage);

        ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(file)); // create output stream to write the table to the file
        outputStream.writeObject(this); // write the table object to the file
        outputStream.close(); // close the output stream to release resources
    }

    public String getPrimaryType() {
        return htblColNameType.get(primaryKey);
    }


    /*
     * @param: Hashtable<String, Object> htblColNameValue
     * @returns: Vector of int[] where each int[] is the page and tuple index of a record that matches the given values
     */
    public Vector<int[]> doesRecordExist(Hashtable<String, Object> htblColNameValue, Object primaryKeyValue) throws IOException, ClassNotFoundException, DBAppException { // check if record exists in the table

        Vector<int[]> resultVector = new Vector<>();

        //Handle searching without key
        if (primaryKeyValue == null) { // primary key value was not found
            resultVector = findRecordByValues(htblColNameValue); // search for the record by values only

        } else { // primary key value was found
            int[] result = findRecordByKey(primaryKeyValue); // search for the record by key
            if (!(result[0] == -1 || result[1] == -1)) { // record was found
                resultVector.add(result); // add the result to the result vector
            }
        }
        return resultVector;
    }

    public void insertTuple(Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException { // insert tuple (record) in the table (file)

        // Find the primary key value in the record
        Object primaryKeyValue = htblColNameValue.get(primaryKey); // get primary key value from the input tuple;
        if (primaryKeyValue == null) { // check if primary key is in the input tuple
            throw new DBAppException("Primary key not found in input tuple");
        }

        if (areColumnValuesInvalid(htblColNameValue)) { // check if column value is valid
            throw new DBAppException("Invalid data type for a column or value out of range");
        }

        Vector<int[]> resultVector = doesRecordExist(htblColNameValue, primaryKeyValue); // vector of int[] where each int[] is the page and tuple index of a record that matches the given values

        if (resultVector.size() > 0) { // check if record already exists in the table
            throw new DBAppException("Record already exists!");
        }

        Tuple tuple = new Tuple(htblColNameType.size(), primaryKeyValue); // create new tuple (record) with the size of the table columns

        int columnIndex = 0;
        for (Map.Entry<String, String> entry : htblColNameType.entrySet()) { // loop over all columns in the table (file)
            String columnName = entry.getKey(); // get column name
            Object columnValue = htblColNameValue.get(columnName); // get column value
            if (columnValue == null) { // check if column value is null
                columnValue = new NullWrapper(); // initialize column value to null wrapper object (to be able to save null in the file)
            }
            tuple.insertValue(columnIndex++, columnValue); // insert column value in the tuple (postfix increment to use the current value then increment)
        }

        if (this.lastPage == -1) { // check if there are no pages in the table
            this.createNewPage(tuple); // create new page (file) and change table metadata
        } else {
            this.addTupleToPage(tuple); // add tuple to the current page (file)
        }
    }

    private boolean areColumnValuesInvalid(Hashtable<String, Object> htblColNameValue) { // check if column value is valid
        for (String columnName : htblColNameValue.keySet()) { // loop over all columns in the input tuple
            Object columnValue = htblColNameValue.get(columnName); // get column value
            String columnType = this.htblColNameType.get(columnName); // get column type
            if (!isValidValueType(columnValue, columnType)) { // check if column value is valid to its type
                return true;
            }
            if (columnValue.toString().compareTo(htblColNameMin.get(columnName)) < 0 ||
                    columnValue.toString().compareTo(htblColNameMax.get(columnName)) > 0) { // check if column value is valid to its min and max
                return true;
            }
        }
        return false;
    }

    private boolean isValidValueType(Object value, String type) { // check if column value is valid
        if (type.equals(DATE_TYPE) && value instanceof String) { // if type is DATE_TYPE, check if value is a string with valid format
            String dateFormat = "\\d{4}-\\d{2}-\\d{2}"; // date format is yyyy-MM-dd
            return ((String) value).matches(dateFormat); // check if value matches the date format
        }
        return switch (type) { // check if value is of the correct type
            case INT_TYPE -> value instanceof Integer;
            case STRING_TYPE -> value instanceof String;
            case DOUBLE_TYPE -> value instanceof Double;
            case DATE_TYPE -> value instanceof Date;
            default -> false;
        };
    }

    private void addTupleToPage(Tuple tuple) throws IOException, ClassNotFoundException, DBAppException { // add tuple to the current page (file)
        //If table has no pages is handled in insertTuple
        int pageToInsert = findPageByKey(tuple.getPrimaryKey()); // Find page to insert in (binary search

        File pageFile = new File(this.directoryPath + this.tableName + "_P" + pageToInsert + ".class"); // create file object with the path of the page (file)
        try (ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(pageFile))) { // create input stream to read the page from the file
            Object[] minMax;

            Page currentPage = (Page) objectInputStream.readObject(); // read the page from the file
            if (currentPage.isFull()) { // if page is full then take the last tuple in the page and insert it
                Tuple lastTuple = currentPage.getTuples().lastElement();
                if (( (Comparable)lastTuple.getPrimaryKey() ).compareTo(( (Comparable)tuple.getPrimaryKey() )) < 0) // check if the last tuple in the page has the same primary key as the tuple to insert
                    overFlowTuple(tuple, pageToInsert + 1); // add tuple to the next page
                else {
                    overFlowTuple(lastTuple, pageToInsert + 1); // add last tuple to the next page
                    currentPage.deleteByIndex(currentPage.getSize()-1); // decrement number of tuples in the page (delete last tuple)
                    minMax = currentPage.insertTuple(tuple); // insert tuple in the page
                    minPrimaryKeyOfEachPage.set(pageToInsert, minMax[0]); // update min primary key of the page
                    maxPrimaryKeyOfEachPage.set(pageToInsert, minMax[1]); // update max primary key of the page
                }
            } else {
                minMax = currentPage.insertTuple(tuple); // insert tuple in the page
                minPrimaryKeyOfEachPage.set(pageToInsert, minMax[0]); // update min primary key of the page
                maxPrimaryKeyOfEachPage.set(pageToInsert, minMax[1]); // update max primary key of the page
            }
        }
    }

    //DONE: Used in case of overflow when inserting new tuples
    private void overFlowTuple(Tuple tuple, int pageToInsert) throws IOException, ClassNotFoundException, DBAppException {
        //If pageToInsert is after last page create new page and put tuple in it
        if (pageToInsert > this.lastPage) { // check if the page is full
            createNewPage(tuple); // create new page (file)
            return;
        }

        File pageFile = new File(this.directoryPath + this.tableName + "_P" + pageToInsert + ".class"); // create file object with the path of the page (file)
        try (ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(pageFile))) { // create input stream to read the page from the file
            Page currentPage = (Page) objectInputStream.readObject(); // read the page from the file
            Object[] minMax;
            if (currentPage.isFull()) { // if page is full then take the last tuple in the page and insert it
                Tuple lastTuple = currentPage.getTuples().lastElement();
                if (( (Comparable) lastTuple.getPrimaryKey()).compareTo(( (Comparable)tuple.getPrimaryKey())) < 0) // check if the last tuple in the page has the same primary key as the tuple to insert
                    overFlowTuple(tuple, pageToInsert + 1); // add tuple to the next page
                else {
                    overFlowTuple(lastTuple, pageToInsert + 1); // add last tuple to the next page
                    currentPage.deleteByIndex(currentPage.getSize()-1); // decrement number of tuples in the page (delete last tuple)
                    minMax = currentPage.insertTuple(tuple); // insert tuple in the page
                    minPrimaryKeyOfEachPage.set(pageToInsert, minMax[0]); // update min primary key of the page
                    maxPrimaryKeyOfEachPage.set(pageToInsert, minMax[1]); // update max primary key of the page
                }
            } else{
                minMax = currentPage.insertTuple(tuple); // insert tuple in the page
                minPrimaryKeyOfEachPage.set(pageToInsert, minMax[0]); // update min primary key of the page
                maxPrimaryKeyOfEachPage.set(pageToInsert, minMax[1]); // update max primary key of the page
            }
        }
    }

    //EDIT: whenever we want to create new page we want to add a tuple in it so we can pass it
    private void createNewPage(Tuple tuple) throws IOException { // create new page (file)
        this.lastPage++; // increment last page
        this.minPrimaryKeyOfEachPage.add(tuple.getPrimaryKey()); // add the first key in the new page to the first keys list
        this.maxPrimaryKeyOfEachPage.add(tuple.getPrimaryKey()); // add the first key in the new page to the last keys list

        // Create new page with required tuple and save it
        Page newPage = new Page(directoryPath + tableName + "_P" + lastPage + ".class"); // create a new page (file)
        try {
            newPage.insertTuple(tuple); // insert tuple in the new page no need to put min and max keys since already done in top
        } catch (DBAppException e) {
            // Can't happen because we just created the page
            System.out.println("Newly Created Page is full size!!! Check for bugs");
        }
        saveTableToFile(); // save table
    }

    private int findPageByKey(Object targetPrimaryKey) { // find page by key
        Comparator<Object> pkComparator = (pk1, pk2) -> ((Comparable)pk1).compareTo((Comparable)pk2);

        int requiredPage = Collections.binarySearch(minPrimaryKeyOfEachPage, targetPrimaryKey, pkComparator); // search for the key in the first keys list

        if (requiredPage < 0) // if key is not found in the first keys list
            requiredPage = -(requiredPage + 1) - 1; // get the index of the page that has the key (the page that is before the key in the first keys list)
        
        if (requiredPage < 0) // in case inserting key smaller than first key
            requiredPage = 0;

        return requiredPage; // return result
    }

    /*
     * @param: Object targetPrimaryKey
     * @returns: int[] result = {page, tuple} if record is found else {-1, -1} if record is not found (page and tuple indices)
     */
    private int[] findRecordByKey(Object targetPrimaryKey) throws IOException, ClassNotFoundException { // find record by key
        int[] result = {-1, -1}; // initialize result array. this will be used to store the indices of the page and tuple where the record is found.

        int requiredPage = findPageByKey(targetPrimaryKey); // find page by key

        File pageFile = new File(directoryPath + tableName + "_P" + requiredPage + ".class"); // create file object with the path of the page (file)
        if (!pageFile.exists()) { // check if page (file) exists
            return result; // return {-1, -1} if record is not found
        }
        try (ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(pageFile))) { // create input stream to read the page from the file
            Page currentPage = (Page) objectInputStream.readObject(); // read the page from the file
            Vector<Tuple> tupleVector = currentPage.getTuples(); // get tuples from the page

            Tuple targetTuple = new Tuple(0,targetPrimaryKey); // create tuple with the target primary key
            int requiredRecord = Collections.binarySearch(tupleVector, targetTuple); // search for the key in the tuples list

            if (requiredRecord >= 0){ // if key is not found in the tuples list)
                result[0] = requiredPage; // set page index in the result array
                result[1] = requiredRecord; // set tuple index in the result array
            }
        }
        return result;
    }


    /*
     * @param: Hashtable<String, Object> htblColNameValue
     * @returns: Vector of int[] where each int[] is the page and tuple index of a record that matches the given values
     */
    private Vector<int[]> findRecordByValues(Hashtable<String, Object> htblColNameValue) throws IOException, ClassNotFoundException, DBAppException { // find record by values
        Vector<int[]> result = new Vector<>(); // initialize result vector. this will be used to store the indices of the page and tuple where the record is found.

        // Check if all columns exist in the table schema
        for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) { // loop over all columns passed in the method
            String columnName = entry.getKey(); // get column name
            if (this.htblColNameType.get(columnName) == null) {
                throw new DBAppException("Column " + columnName + " does not exist");
            }
        }

        for (int pageIndex = 0; pageIndex <= lastPage; pageIndex++) { // loop over all pages
            File pageFile = new File(directoryPath + tableName + "_P" + pageIndex + ".class"); // create file object with the path of the page (file)

            try (ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(pageFile))) { // create input stream to read the page from the file
                Page currentPage = (Page) objectInputStream.readObject(); // read the page from the file
                Vector<Tuple> tupleVector = currentPage.getTuples(); // get tuples from the page

                for (int tupleIndex = 0; tupleIndex < tupleVector.size(); tupleIndex++) { // loop over all tuples in the page
                    Tuple tuple = tupleVector.get(tupleIndex); // get tuple from the page

                    boolean tupleFound = true; // initialize tupleFound flag
                    int columnIndex = 0; // initialize column index to 0
                    for (String entry : htblColNameType.keySet()) { // loop over all columns passed in the method
                        if (htblColNameValue.get(entry) == null) { // check if column value is null
                            columnIndex++;
                            continue;
                        }
                        if (!tuple.getValue(columnIndex++).equals(htblColNameValue.get(entry))) { // check if tuple value is not equal to the target value
                            tupleFound = false;
                            break;
                        }
                    }

                    if (tupleFound) {
                        int[] match = {pageIndex, tupleIndex}; // create array to store the indices of the page and tuple where the record is found.
                        result.add(match); // add the array to the result vector
                    }
                }
            }
        }

        return result;
    }

    public void updateTuple(Object objKey, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException { // update tuple (record)

        if (areColumnValuesInvalid(htblColNameValue)) { // check if column value is valid
            throw new DBAppException("Invalid input");
        }

        int[] recordIndices = findRecordByKey(objKey); // find record by key

        if (recordIndices[0] == -1 || recordIndices[1] == -1) { // check if record is not found
            return; // return if record is not found
        }

        String pagePath = directoryPath + tableName + "_P" + recordIndices[0] + ".class"; // get page (file) path
        File file = new File(pagePath); // create file object with the path of the page (file)
        ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(file)); // create input stream to read the page from the file
        Page curPage = (Page) objectInputStream.readObject(); // read the page from the file

        Tuple updatedTuple = new Tuple(htblColNameType.size(), objKey); // create new tuple (record) with the size of the input tuple
        int i = 0;
        for (String columnName : htblColNameValue.keySet()) { // loop over all columns in the input tuple
            if (columnName.equals(primaryKey))
                if (!htblColNameValue.get(columnName).equals(objKey))
                    throw new DBAppException("Cannot update primary key!!");

            if (htblColNameValue.get(columnName) == null) { // check if column value does not exist
                updatedTuple.insertValue(i++, curPage.getTuples().get(i)); // insert old data if not found
            } else
                updatedTuple.insertValue(i++, htblColNameValue.get(columnName)); // insert column value in the tuple
        }


        curPage.getTuples().set(recordIndices[1], updatedTuple); // set tuple in the page
        objectInputStream.close(); // close input stream to read the page from the file
        curPage.writePage(); // write page (file)
    }


    public void deleteTuple(Hashtable<String, Object> htblColNameValue) throws Exception { // delete tuple (record) from the table (file)
        //TODO: should  we delete all matching pairs or just the first one?
        Object key = htblColNameValue.get(primaryKey); // get primary key value from the input tuple

        int[] recordIndices = {-1, -1};
        Vector<int[]> recordIndicesVector = new Vector<>();
        if (key == null) // check if primary key is in the input tuple
            recordIndicesVector = findRecordByValues(htblColNameValue); // find record by values if key is not existent
        else
            recordIndices = findRecordByKey(key); // find record by key if it exists

        if (recordIndices[0] == -1 && recordIndicesVector.size() == 0) // check if record exists in the table
            return; // return if record is not found
            // throw new DBAppException("The tuple you're trying to delete doesn't exist");

        if (key != null) 
            recordIndicesVector.add(recordIndices);

        Collections.reverse(recordIndicesVector); // reverse the vector to delete from the last page first

        for (int[] recordIndex : recordIndicesVector) { // loop over all records found    
            int targetPageIndex = recordIndex[0]; // get page index
            int tupleIndex = recordIndex[1]; // get tuple index

            // Get page that we want to delete from
            String pagePath = directoryPath + tableName + "_P" + targetPageIndex + ".class"; // get page (file) path
            File file = new File(pagePath); // create file object with the path of the page (file)
            ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(file)); // create input stream to read the page from the file
            Page currentPage = (Page) objectInputStream.readObject(); // read the page from the file

            Object[] minMax = currentPage.deleteByIndex(tupleIndex); // remove tuple from the page
            minPrimaryKeyOfEachPage.set(targetPageIndex, minMax[0]); // update min primary key of the page
            maxPrimaryKeyOfEachPage.set(targetPageIndex, minMax[1]); // update max primary key of the page
            
            objectInputStream.close(); // close input stream to read the page from the file

            if (currentPage.isEmpty()) { // check if page is empty shift all pages one number up
                minPrimaryKeyOfEachPage.removeElementAt(targetPageIndex); // update min primary key of the page
                maxPrimaryKeyOfEachPage.removeElementAt(targetPageIndex); // update max primary key of the page

                for (int page = targetPageIndex + 1; page <= lastPage; page++) {
                    String oldPath = directoryPath + tableName + "_P" + (page) + ".class"; // get page to be shifted path
                    String newPath = directoryPath + tableName + "_P" + (page - 1) + ".class"; // get page to be shifted path
                    File newFile = new File(oldPath); // create file object with the path of the page (file)
                    ObjectInputStream objectInputStream1 = new ObjectInputStream(new FileInputStream(newFile)); // create input stream to read the page from the file
                    Page pageToBeShifted = (Page) objectInputStream1.readObject(); // read the page from the file
                    pageToBeShifted.setPath(newPath);

                    objectInputStream1.close(); // close input stream to read the page from the file
                }
                File filetodelete = new File(directoryPath + tableName + "_P" + (lastPage--) + ".class"); // get page to be shifted path)lastPage--;
                boolean isdeleted = filetodelete.delete();
                System.out.println(isdeleted);
            }
        }
        saveTableToFile(); // save table to file
    }



    public static boolean tableExists(String directoryPath, String strTableName) { // check if table (file) exists
        String directory = directoryPath + strTableName; // table directory path
        String fileName = strTableName + ".class"; // table file name
        File tableDir = new File(directory + "/" + fileName); // create file object with the path of the table
        return tableDir.exists(); // check if the file exists
    }
}
