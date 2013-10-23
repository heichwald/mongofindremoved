mongofindremoved
================

Help you find deleted/removed documents from your binary data files after an accidental remove all from a collection
(Usually by default under /data/db/)

If you drop the db, disconnect the drive or ensure that nothing writes on the disk and try to recover the files with a filesystem recovery tool.

Required:
Java 7

Just run the main in ParseBinary.java
Modify this java class to enter your mongo info: server, dbs, collections, data file names
