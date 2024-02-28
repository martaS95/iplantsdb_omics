# -*- coding: utf-8 -*-

# Import required modules
from Bio import Entrez
import GEOparse
import re
import requests

from configparser import RawConfigParser

db_configs = RawConfigParser()
db_configs.read('/iplantsdb_omics/conf/iplantsdb_omics.conf')

Entrez.email = str(db_configs.get('iplants-omics-configurations', 'entrez_email')),


class Extract:
    """
    This class extracts data from external sources such as NCBI GEO and BioStudies databases through APIs to a staging
    area that corresponds to the attribute path (a folder in the computer)
    """

    def __init__(self, organism: str, data_type: str, study_type: str):
        """
        :param organism: organism filter to perform a search on GEO and ArrayExpress collection on BioStudies databases
        :param data_type: dataset study type filter to perform a search on GEO database
        :param study_type: dataset study type filter to perform a search on ArrayExpress collection
        """

        self.organism = organism
        self.data_type = data_type
        self.study_type = study_type
        self.path = "staging_area/"

    def get_geo_accession(self) -> list:
        """
        :return: retrieve a list of dataset accession numbers from NCBI GEO database, which is as result of the filter
        combination of organism and dataset type
        """

        query = f"{self.organism} [Organism] AND {self.data_type} [DataSet Type]"

        # Search, using history results if cached before
        handle = Entrez.esearch(db="gds", term=query, usehistory=True, retmax=1000)
        record = Entrez.read(handle)
        list_ids = record["IdList"]
        handle.close()
        uid_regex = re.compile("[1-9]+0+([1-9]+\d*)")
        gse_list = ["GSE" + uid_regex.match(uid).group(1) for uid in list_ids]
        return gse_list

    def download_softfiles(self) -> None:
        """
        This method uses a list of GEO accession numbers to extract the correspondents softfiles from NCBI GEO database
        """
        for access in self.get_geo_accession():
            GEOparse.get_GEO(geo=access, destdir=self.path)  # Get GSE object

    def get_arrayexpress(self) -> list:
        """
        This method perform a restricted search by organism and study type on ArrayExpress collection through a REST API
        :return: a list with the ArrayExpress accession numbers
        """
        api_url = ('https://www.ebi.ac.uk/biostudies/api/v1/arrayexpress/search?pageSize=100&study_type="' +
                   self.study_type + '"&organism="' + self.organism + '"')
        response = requests.get(api_url)
        file = response.json()
        info = file["hits"]
        accession = []
        for i in info:
            accession.append(i["accession"])
        return accession

    def compare_accessions_array(self) -> list:
        """
        This method uses a list of dataset's GEO and a list of ArrayExpress accession numbers to compare the
        common ones. Based on that, the files with the common ones are extracted by GEO methods and not by ArrayExpress.
        :return: a list with ArrayExpress accession numbers (not in common with GEO)
        """
        accessions = []
        geo = self.get_geo_accession()
        for acc in self.get_arrayexpress():
            x = re.sub(r"E-GEOD-", "GSE", acc)
            if not any(x in s for s in geo):
                accessions.append(x)
        return accessions

    def download_sdrf(self) -> None:
        """
        This method uses a list of ArrayExpress accession numbers not in common with GEO's to download the
        correspondents' metadata datasets in sdrf files
        """
        for accession in self.compare_accessions_array():
            url = "https://www.ebi.ac.uk/biostudies/files/" + accession + "/" + accession + ".sdrf.txt"
            r = requests.get(url, allow_redirects=True)
            open(self.path + accession + ".sdrf.txt", "wb").write(r.content)


if __name__ == "__main__":
    extract = Extract("Vitis vinifera", "Expression profiling by high throughput sequencing",
                      "RNA-seq of coding RNA")
    extract.download_sdrf()
    extract = Extract("Vitis vinifera", "Expression profiling by array",
                      "transcription profiling by array")
    extract.download_sdrf()
