# External merge sort

## Summary

This project is for performing external merge sort.

### External merge sort
 
This module has external merge sort. An external merge sort allows you to sort data 
present at **a source location** with the user provided comparator logic 
and stores back the sorted files at the **specified destination location**. 
This can be done in limited memory.

**a source location** and **specified destination location** can be one of 
the followings (a different source and destination is possible).
 - A file system
 - Stream of records.
 
 Other configurations in the input are accepted in `com.mozafaq.extmergesort.Configuration`.
 The configuration object can be constructed from properties file using 
 `com.mozafaq.extmergesort.PropertiesParser`. Example
 of input parameters and their significance is present in the example file 
 [here](src/main/resources/sample-input.properties).
 
 A file can be in any format and one need to provide followings for
 sorting (other than file object location)
 - Comparator logic implements Comparator interface
 - Logic to read a record from `InputStream`
 - Logic to write a record to `OutputStream`

One can provide custom logic by implementation of 
interface `com.mozafaq.extmergesort.SortHandleProvider`

## Build

 ```mvn clean insall```

## Contributors
 - Mozaffar Afaque