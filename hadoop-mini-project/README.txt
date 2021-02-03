# Summary of Files
* data.csv - input file of 16 car transactions of various types (I = initial sale, R = repair, A = accident)
* autoinc_mapper1.py - 1st mapper keeps just the columns of interest (VIN, transaction type, make, year)
* autoinc_reducer1.py - 1st reducer copies the make and year from the I transactions to the A transactions, and then keeps just the A transactions (4)
* autoinc_mapper2.py - 2nd mapper removes VIN, concatenates make and year into a new key, and creates a new count column of 1 for each accident
* autoinc_reducer2.py - 2nd reducer groups the count by make/year (final result: 2015 Mercedes = 2 accidents, 2016 Mercedes = 1 accident, 2003 Nissan = 1 accident)
* hdfs_input.png - image of path to input on HDFS
* hdfs_output_all_accidents.png - image of path to output/all_accidents on HDFS
* hdfs_output_make_year_count.png - image of path to output/make_year_count (final result)
* mr_log.txt - log from running the MapReduce job

# How to Reproduce
* https://www.youtube.com/watch?v=735yx2Eak48&ab_channel=BinodSumanAcademy - follow these instructions to:
    * install Oracle VM VirtualBox
    * install HortonWorks Data Platform (HDP) on Hortonworks Sandbox
    * setup accounts to use HDP on VirtualBox
* HDFS interface - http://127.0.0.1:8080/#/login
    * login and go to Files View
    * create a folder for this project
    * in the project folder:
        * create an input folder and upload data.csv there
        * create an empty output folder
* Sandbox command line - http://127.0.0.1:4200/
    * login and create a project folder with the same name as above
    * add the 4 python and 1 sh files here
    * create an input folder and add the data.csv file there
    * create an empty output folder
    * run the sh file ($ sh automobile-mr.sh)
* check the output folder in the HDFS interface and see the 2 output subfolders are created
