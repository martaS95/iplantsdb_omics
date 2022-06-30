This folder contains:
* Extract.py - Python file that contains a class which extracts data from external sources such as NCBI GEO and BioStudies databases through APIs to a staging
    area
* Transform.py - Python file that contains which transforms the data from the staging area (obtain from the Extract class) into a specific format, a JSON file.
* Mongo_schema_Load.py - Python file that contains a class which implements the MongoEngine, which provides a model class to define a document schema, to easily map Python objects into the target database on MongoDB, facilitating the communication with it.
* MongoCRUD.py - Python file with a method that performs the target database update if a new accession number is added.
* workflow_manager.py - Python file that contains Tasks for Luigi runs
* staging_area : a folder with all the extracted files
* data : a folder that contains all the output files created when Luigi runs
