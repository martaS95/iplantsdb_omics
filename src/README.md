![logo_escola_eng](https://user-images.githubusercontent.com/96354274/176214605-90adc5b2-1813-4de8-99ae-37421456769c.png)

# Development-of-an-ETL-pipeline-for-plant-omics-data

## Context
The work present in this repository was developed within the scope of the Bioinformatics and Systems Biology project, to support the development of the project 
"Development of an ETL pipeline for plant omics data".
The main objective of this project is to apply ETL tools and develop a pipeline to extract, transform and load the metadata related to the plant omics datasets available 
in different databases. The main transcriptomics databases that will be use on the current project are GEO and *ArrayExpress*. Transformed data will be load into a 
document-oriented NoSQL database managed by the MongoDB system.

## Abstract
Omics data, particularly transcriptomics, have been widely used in the study of plant metabolic pathways. Hence, this project implements an ETL pipeline to extract and 
integrate metadata from transcriptomics datasets of *Vitis vinifera* into a comprehensive repository of plant metabolic data, which is the basis for reconstructing plant 
metabolic models. Data was extracted from two sources, GEO and *ArrayExpress*, with different formats, and transformed into a common structure defined.
Then, the transformed metadata was loaded into a document-oriented NoSQL database managed by the MongoDB system. Finally, Luigi was applied to sequentially execute the 
ETL pipeline and facilitate the update of the target database.

The main result of this project is the ETL pipeline to extract data and update the *Transcriptomics* collection in the target MongoDB repository, which is schematised with the main steps and outputs.



![esquema](https://user-images.githubusercontent.com/96354274/176487185-23a6e1b6-3341-4e5c-bb8e-8db810ca8621.png)
