The artifacts contained herein have dummy values for database hostnames, etc. 
This file contains helpful scripts and templates to automatically set properties to desired values.
------------------------------------

Set your properties in ../PDI/SuperJobParameters.properties, then run the groovy script:
groovy gen_model.groovy

This will create a metadata model from the template with your connection information filled in.
