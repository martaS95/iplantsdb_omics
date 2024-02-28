# Import required modules
import json
from mongoengine import *
from datetime import datetime

from configparser import RawConfigParser

db_configs = RawConfigParser()
db_configs.read('/iplantsdb_omics/conf/iplantsdb_omics.conf')

# Connects to target database by authenticating

MONGODB_DATABASE = {
    "name": str(db_configs.get('iplants-omics-configurations', 'mongodb_name')),
    "host": str(db_configs.get('iplants-omics-configurations', 'mongodb_host')),
    "port": int(db_configs.get('iplants-omics-configurations', 'mongodb_port')),
}

disconnect()
connect(MONGODB_DATABASE["name"], host=MONGODB_DATABASE["host"],
        port=MONGODB_DATABASE["port"])


class Transcriptomics(Document):
    """
    This class implements the MongoEngine which provides a model class to define a document schema, to easily map Python
    objects into the target database on MongoDB, facilitating the communication with it.
    """

    database = StringField()
    title = StringField()
    data_type = StringField(required=True)
    organism = StringField(required=True)
    accession_number = StringField(primary_key=True)
    platform_id = ListField(StringField())
    contributors = ListField(StringField(), allow_empty=True)
    last_update_date = DateTimeField(required=True, default=datetime.utcnow)
    overall_design = StringField()
    samples = DictField()


def read_json_database(filename: str):
    """
    This function opens a JSON file obtained from the Transform class
    :param filename: name of the json file which contains the datasets and returns its data
    """

    with open(filename, encoding="mbcs") as json_file:
        data = json.load(json_file)
        return data


def load_data(data_dic: dict) -> None:
    """
    :param data_dic: dataset's data from the JSON file
    """

    for key, value in data_dic.items():
        doc = Transcriptomics(**value)
        doc.save()


if __name__ == "__main__":
    data = Transcriptomics()
    # data.drop_collection()

    db_file = "data/dataset.json"
    transcript_data = read_json_database(filename=db_file)
    load_data(data_dic=transcript_data)
