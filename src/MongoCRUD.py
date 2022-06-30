# Import required modules
import Mongo_schema_Load
from Mongo_schema_Load import Transcriptomics
from mongoengine import *


def update_mongo(new_data):
    """"
    This method performs the target database update if a new accession number is added
    """
    for key, value in new_data.items():
        new_doc = Transcriptomics(**value)
        if DoesNotExist:  # when a new entry is added
            print("NEW ENTRY INSERTED")
            new_doc.save()


if __name__ == "__main__":
    disconnect()
    connect(host="mongodb://palsson.di.uminho.pt:1017/plantcyc")

    data = Mongo_schema_Load
    db_file = "data/dataset.json"
    transcript_data = data.read_json_database(filename=db_file)
    update_mongo(transcript_data)
