# -*- coding: utf-8 -*-

# importing required modules
from os.path import isfile, join
import GEOparse
from os import listdir
import json
import requests
from Extract import Extract


class Transform:
    """
    This class transforms the data from the staging area (obtain from the Extract class) into a specific format
    """

    def __init__(self) -> None:
        """
        :param file_path: path to the folder (staging area) with all the files extracted by the Extract class
        """
        self.dictionaryPrincipal = {}
        self.index = 0

        self.file_path = "staging_area/"

    def open_files(self, type) -> list:
        """
        This method go to the staging area and lists the files in it
        :param type: file's type to retrieve
        :return: list of all files in the folder (staging area)
        """
        allfiles = [f for f in listdir(self.file_path) if isfile(join(self.file_path, f)) and type in f]
        return allfiles

    def createFile(self) -> None:
        """
        This method creates a JSON file containing the datasets from the GEO and ArrayExpress. For each dataset, the
        information retrieved is organized into a dictionary and includes:
        - database: database name from which the datasets were extracted
        - title: title of the dataset
        - data_type: dataset study type
        - organism: organism in study
        - accession_number: the accession number for each dataset
        - platform_id: platform identifier that was used to perform gene expression analysis
        - contributors: list of contributors that are present in the study for each dataset
        - last_update_date: date of the last update
        - overall design: descriptive information about the samples and the overall study
        - samples: dictionary where the keys are samples id's and the values corresponds to the samples descriptions
        """
        with open("data/dataset.json", "w", encoding="utf-8") as outfile:
            json.dump(self.dictionaryPrincipal, outfile, ensure_ascii=False)

    def transformGEO(self) -> None:
        """
        This method transforms the GEO's datasets extracted.
        """

        # There are 3 main components of each GSE (Series) object: a dictionary of GSM (Samples) objects,
        # a dictionary of GPL (Platforms) objects and its own metadata
        for file in self.open_files(".soft.gz"):
            filepath = self.file_path + file
            gse = GEOparse.get_GEO(filepath=filepath)
            dictionaryTemp = {
                "database": "",
                "title": "",
                "data_type": "",
                "organism": "",
                "accession_number": "",
                "platform_id": [],
                "contributors": [],
                "last_update_date": "",
                "overall_design": "",
                "samples": {}
            }

            # GSE (Series) is an original submitter-supplied record that summarizes the study, including samples and
            # platforms
            if "GSE" in gse.metadata["geo_accession"][0]:
                dictionaryTemp["database"] = "GEO"

            dictionaryTemp["title"] = gse.metadata["title"][0]
            dictionaryTemp["data_type"] = " , ".join(gse.metadata["type"])

            # GPL (Platform) contains a tab-delimited table containing the array definition
            for gpl_name, gpl in gse.gpls.items():
                dictionaryTemp["organism"] = gpl.metadata["organism"][0]

            dictionaryTemp["accession_number"] = gse.metadata["geo_accession"][0]
            dictionaryTemp["platform_id"] = gse.metadata["platform_id"]

            if "contributor" in gse.metadata:
                dictionaryTemp["contributors"] = gse.metadata["contributor"]

            strDate = gse.metadata["last_update_date"][0]
            dictionaryTemp["last_update_date"] = strDate

            dictionaryTemp["overall_design"] = gse.metadata["overall_design"][0]

            # GSM (Sample) contains information about the conditions and preparation of a sample
            lista = []
            for gsm_name, gsm in gse.gsms.items():
                if "description" in gsm.metadata:
                    lista.append(" ".join(gsm.metadata["description"]))
                else:
                    lista.append("")

            dictionaryTemp["samples"] = {}
            for i in range(len(gse.metadata["sample_id"])):
                id = gse.metadata["sample_id"][i]
                for j in range(len(lista)):
                    dictionaryTemp["samples"][id] = lista[i]

            x = "dataset" + str(self.index + 1)
            self.dictionaryPrincipal[x] = dictionaryTemp.copy()

            self.index += 1

    def transformArrayExpress(self, organism, data_type_geo, data_type_AE):
        """
        This method transforms the extracted ArrayExpress datasets.
        """
        extract = Extract(organism, data_type_geo, data_type_AE)
        accessions = extract.compare_accessions_array()

        for accession in accessions:
            dictionaryTemp = {
                "database": "",
                "title": "",
                "data_type": "",
                "organism": "",
                "accession_number": "",
                "platform_id": [],
                "contributors": [],
                "last_update_date": "",
                "overall_design": "",
                "samples": {}
            }
            api_url = "https://www.ebi.ac.uk/biostudies/api/v1/studies/" + accession
            response = requests.get(api_url)
            file = response.json()

            for i in file["section"]["subsections"]:
                try:
                    if i["type"] == "Author":
                        dictionaryTemp["contributors"].append(i["attributes"][0]["value"])
                except:
                    pass

            dictionaryTemp["accession_number"] = file["accno"]
            dictionaryTemp["title"] = file["attributes"][0]["value"]
            dictionaryTemp["database"] = file["attributes"][3]["value"]
            dictionaryTemp["data_type"] = file["section"]["attributes"][1]["value"]
            dictionaryTemp["organism"] = file["section"]["attributes"][2]["value"]
            dictionaryTemp["overall_design"] = file["section"]["attributes"][3]["value"]
            dictionaryTemp["last_update_date"] = None

            tempSourceName = None
            tempDescription = None
            tempPlatform = None
            f = open(self.file_path + accession + ".sdrf.txt", "r")
            for line in f.read().splitlines():
                line = line.split("\t")
                try:  # only runs the first line
                    tempSourceName = line.index("Source Name")
                    try:
                        tempDescription = line.index("Description")
                    except:
                        tempDescription = None
                    try:
                        tempPlatform = line.index("Comment [Platform_title]")
                    except:
                        tempPlatform = None
                except:
                    samples_id = line[tempSourceName]
                    dictionaryTemp["samples"][samples_id] = ""
                    if tempDescription is not None:
                        dictionaryTemp["samples"][samples_id] = line[tempDescription]

                    if tempPlatform is not None:
                        dictionaryTemp["platform_id"].append(line[tempPlatform])

            x = "dataset" + str(self.index + 1)
            self.dictionaryPrincipal[x] = dictionaryTemp.copy()

            self.index += 1


if __name__ == "__main__":
    transf = Transform()
    transf.transformGEO()
    transf.transformArrayExpress("Vitis vinifera", "Expression profiling by high throughput sequencing",
                                 "RNA-seq of coding RNA")
    transf.transformArrayExpress("Vitis vinifera", "Expression profiling by array", "transcription profiling by array")
    transf.createFile()
