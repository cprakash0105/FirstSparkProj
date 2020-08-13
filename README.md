# FirstSparkProj
There are two programs in this project as of now.

First - GenericType1 : 
In this project, I have implemented a SCD type 1 which we so often use in DWH
This code utilizes an input table from Postgresql as a full file and an input file as delta file.
It creates a final DF after comparing and merging the changes.

Secod - GenericType2 :
This project is quite similar to the first one in terms of impelementation. Howver, this gets a little
complicated with joins as I have tried to implement a SCD Type 2 functionality here.
