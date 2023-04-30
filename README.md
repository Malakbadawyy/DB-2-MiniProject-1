## Test cases:
* Test Cases:
* [x] Create
* [ ] insert
* [ ] update
* [ ] delete

* Creates:
    * [x] Create table
    * [x] Create table that already exists
    * [x] Create table with invalid data types (rejected)
    * [x] Create table with inconsistent columns (rejected)

* Inserts:
    * [x] Insert into full page
    * [x] Insert into last record (full page)
    * [x] Insert into first record (full page)
    * [x] Insert value between last record and next first record
    * [x] Insert record with no primary key (rejected)
    * [x] Insert record that already exists (rejected)
    * [x] Insert record with duplicate clustering in last record of page (rejected)
    * [x] Insert record with different columns than table (rejected)
    * [ ] Insert record with different data types than table (rejected)
    * [ ] Insert record exceeding max of column (rejected)
    * [ ] Insert record less than min of column (rejected)

* Updates:
    * [ ] Update record
    * [ ] Update clustering key (rejected)
    * [ ] Update multiple columns
    * [ ] Update record with no primary key (rejected)
    * [ ] Update record that doesnt exist (rejected)
    * [ ] Update record with different data types than table (rejected)
    * [ ] Update record with different columns than table (rejected)
    * [ ] Update record exceeding max of column (rejected)
    * [ ] Update record less than min of column (rejected)
    * [ ] Update record to an empty table

* Deletes:
    * [ ] Delete one record by clustering key (binary search)
    * [ ] Delete records by attribute (linear)
    * [ ] Delete multiple records by attribute
    * [ ] Delete record that doesnt exist
    * [ ] Delete from an empty table

* Make sure any page variable is set to null after being done, and use
* System.gc();


