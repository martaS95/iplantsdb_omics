import luigi
from Extract import Extract
import MongoCRUD
import Mongo_schema_Load
from Transform import Transform


# Task 1: Extract filtered data by RNA-Seq study and organism from the GEO database
class ExtractDataGEORNASeq(luigi.Task):

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget("data/ExtractGEORNASeq.txt")

    def run(self):
        extract = Extract("Vitis vinifera", "Expression profiling by high throughput sequencing",
                          "RNA-seq of coding RNA")
        extract.download_softfiles()

        with self.output().open("w") as outfile:
            outfile.write("RNA-Seq study type datasets were extracted from GEO")


# Task 2: Extract filtered data by microarray study and organism from the GEO database
class ExtractDataGEOMicroArray(luigi.Task):

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget("data/ExtractGEOMicroArray.txt")

    def run(self):
        extract = Extract("Vitis vinifera", "Expression profiling by array", "transcription profiling by array")
        extract.download_softfiles()

        with self.output().open("w") as outfile:
            outfile.write("Microarray study type datasets were extracted from GEO")


# Task 3: Extract filtered data by RNA-Seq study and organism from the ArrayExpress collection on BioStudies database
class ExtractData_ArrayExpressRNASeq(luigi.Task):

    def requires(self):
        return ExtractDataGEORNASeq()

    def output(self):
        return luigi.LocalTarget("data/ExtractRNASeq.txt")

    def run(self):
        extract = Extract("Vitis vinifera", "Expression profiling by high throughput sequencing",
                          "RNA-seq of coding RNA")
        extract.download_sdrf()

        with self.output().open("w") as outfile:
            outfile.write("RNA-Seq study type datasets were extracted from ArrayExpress")


# Task 4: Extract filtered data by microarray study and organism from the ArrayExpress collection on BioStudies database
class ExtractData_ArrayExpressMicroArray(luigi.Task):

    def requires(self):
        return ExtractDataGEOMicroArray()

    def output(self):
        return luigi.LocalTarget("data/ExtractMicroArray.txt")

    def run(self):
        extract = Extract("Vitis vinifera", "Expression profiling by array", "transcription profiling by array")
        extract.download_sdrf()

        with self.output().open("w") as outfile:
            outfile.write("Microarray study type datasets were extracted from ArrayExpress")


# Task 5: Transform data retrieved from the previously tasks
class TransformData(luigi.Task):

    def requires(self):
        return ExtractData_ArrayExpressRNASeq(), ExtractData_ArrayExpressMicroArray()

    def output(self):
        return luigi.LocalTarget("data/dataset.json")  # "data/Transform.txt"

    def run(self):
        transf = Transform()
        transf.transformGEO()
        transf.transformArrayExpress("Vitis vinifera", "Expression profiling by high throughput sequencing",
                                     "RNA-seq of coding RNA")
        transf.transformArrayExpress("Vitis vinifera", "Expression profiling by array",
                                     "transcription profiling by array")

        transf.createFile()


# Task 6: Perform the load and update of the target database on Mongodb
class SaveAndUpdateData(luigi.Task):

    def requires(self):
        return TransformData()

    def output(self):
        return luigi.LocalTarget("data/Load.txt")

    def run(self):
        db_file = "data/dataset.json"
        transcript_data = Mongo_schema_Load.read_json_database(filename=db_file)
        MongoCRUD.update_mongo(transcript_data)
        with self.output().open("w") as outfile:
            outfile.write("data loaded into target database on Mongodb")


if __name__ == "__main__":
    # luigi.build([ExtractData()])
    # luigi.build([TransformData()])
    luigi.build([SaveAndUpdateData()])
