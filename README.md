# agrilac-gt-insivumeh

## Folder Structure

### Conf

This folder will keep the necessary files to run the scripts. Each script file  of a programming language have  to have a YML file. Pentaho, instead will have kettle.properties and other paramertes files like CSV.

### Docs
 
 This folder will keep the operations manual of the pipeline.
 
### Models
If the project have a machine learning model or business rule based model. It's necessary to save the final model in this folder with their version in order to keep traceability of model 
.
### Notebooks

This folder will store the jupyter notebooks used to develop and testing features, hypothesis and so on.

### Schedulers

This folder will contain the script that will run the production pipeline

### Scripts

This folder have to have the final scripts that will perform the production pipeline

## General rules

 - Use a well defined name of the file like:
		 {step_number}_{name_of_process}_{action}.{extension}
		 1_icc_quality_process_clean_data.py
 -  Avoid have all the code in one file, remember to divide the files in a proper and manageable steps
 - Don't reinvent the wheel, build reusable code "KEEP IT SIMPLE"
 -  Don't set user variables fixed in the code instead use configuration files
