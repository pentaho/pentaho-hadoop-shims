The artifacts contained herein have dummy values for database hostnames, etc. 
This file contains helpful scripts and procedures to automatically set properties to desired values.
------------------------------------


Linux sed command to set your database hostname for a metdata model (here it is set to my.database.hostname.org):
sed -i "s/my.hostname.com/my.database.hostname.org/g" *

Mac OS X sed command to set your database hostname for a metdata model (here it is set to my.database.hostname.org):
sed -i '' "s/my.hostname.com/my.database.hostname.org/g" *

Alternatively (or on an OS like Windows with no sed alternative), the model can be loaded into PME as-is and you can simply change the Connection properties manually.
