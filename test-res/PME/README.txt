The artifacts contained herein have dummy values for database hostnames, etc. 
This file contains helpful scripts and templates to automatically set properties to desired values.
------------------------------------

For the PME metadata model, set the appropriate property values in ../PDI/SuperJobParameters.properties and run the following command from the PME directory:

groovy gen_model.groovy

This will create an instance of the metadata model, called Inventory_model_HiveImpala.xmi, for the Hive 1 / 2 / Impala connections configured using the above properties file.  From PME you can import this file as a model.

IMPORTANT: In order to use the MQL queries in this directory, make sure to name your domain "pme-jdbc-hive-test", this will allow you to open and run the MQL queries (*.mql files) from the Query Editor Dialog in PME.



Alternatively to the above procedure, you can change the template yourself :

Linux sed command to set your database hostname for a metdata model (here it is set to my.database.hostname.org):
sed -i "s/my.hostname.com/my.database.hostname.org/g" *

Mac OS X sed command to set your database hostname for a metdata model (here it is set to my.database.hostname.org):
sed -i '' "s/my.hostname.com/my.database.hostname.org/g" *

As a last resort (or on an OS like Windows with no sed alternative), the model can be loaded into PME as-is and you can simply change the Connection properties manually.

