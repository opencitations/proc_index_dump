import argparse
import csv
import json
from pandas import read_csv
from zipfile import ZipFile
import os
import datetime


class Log:
    def __init__(self, pname):
        self.dir = ".log"
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)

        time = str(datetime.datetime.now()).replace(" ", "_")
        self.flog = self.dir+"/"+pname+"_"+time+".log"
        if not os.path.exists(self.flog):
            with open(self.flog, 'w') as f:
                f.write("time,msg")

    def w_log(self, msg):
        with open(self.flog, 'a') as f:
            time = str(datetime.datetime.now())
            f.write(time+","+msg+"\n")


class Tmp:
    def __init__(self, pname):
        self.dir = ".tmp"
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)

        self.procdir = self.dir+"/"+pname
        if not os.path.exists(self.procdir):
            os.makedirs(self.procdir)

    def w_tmpfile(self, f_name, content):
        if f_name.endswith(".json"):
            fdest = self.procdir+"/"+f_name
            with open(fdest, 'w') as file:
                json.dump(content, file)


class Out:
    def __init__(self, pname):
        self.dir = "out_"+pname
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)

    def w_outfile(self, f_name, content):
        if f_name.endswith(".json"):
            fdest = self.dir+"/"+f_name
            with open(fdest, 'w') as file:
                json.dump(content, file)


def read_fcsv(fpath):
    reader = csv.DictReader(open(fpath, 'r'))
    return list(reader)


def getentries(coci_dir, dois_csv, log, tmp, out):

    # init the given list of DOIs as dict
    index_dois = dict()
    with open(dois_csv, 'r') as data:
        for line in csv.reader(data):
            doi_val = line[0]
            #normalize doi value
            doi_val = doi_val.strip().lower()
            index_dois[doi_val] = dict()
            index_dois[doi_val]["citations"] = []
            index_dois[doi_val]["references"] = []

    # backup
    checked_index = set()
    bkup_f_index = set()
    log.w_log("BACKUP")
    for bkup_f in os.listdir(tmp.procdir):
        # bkup_f = part_2020-01-13T19_31_19_1-4.zip
        if bkup_f.endswith("json") and bkup_f.startswith("part_"):
            zip_f_name = bkup_f.split(".")[0].replace("part_", "")+".zip"
            bkup_f_index.add(zip_f_name)
            for k, v in json.load(open(tmp.procdir+"/"+bkup_f)).items():
                index_dois[k]["citations"] += v["citations"]
                index_dois[k]["references"] += v["references"]
                if len(v["citations"]) > 0 or len(v["references"]) > 0:
                    checked_index.add(k)

        log.w_log("BACKUP_FILES_DONE: " + str(len(bkup_f_index)))
        log.w_log("BACKUP_DOIS_FOUND: " + str(len(checked_index)))

    # read the dump of COCI
    log.w_log("RUNNING_PROC")
    for archive_name in os.listdir(coci_dir):
        if archive_name.endswith("zip") and archive_name not in bkup_f_index:
            archive_path = os.path.join(coci_dir, archive_name)
            log.w_log("PROCESSING_FILE: "+archive_name)
            file_dois = dict()
            with ZipFile(archive_path) as archive:
                for csv_name in archive.namelist():
                    with archive.open(csv_name) as csv_file:
                        df_data = read_csv(csv_file, encoding='utf-8')
                        # Process the CSV here
                        for k_doi in index_dois:
                            #if k_doi not in checked_index:
                            cits = df_data[df_data['citing']
                                           == k_doi].to_dict('records')
                            refs = df_data[df_data['cited']
                                           == k_doi].to_dict('records')
                            index_dois[k_doi]["citations"] += cits
                            index_dois[k_doi]["references"] += refs

                            if len(cits) > 0 or len(refs) > 0:
                                # update global index
                                file_dois[k_doi] = dict()
                                file_dois[k_doi]["citations"] = cits
                                file_dois[k_doi]["references"] = refs
                                log.w_log("DOI_FOUND: "+k_doi)
                                #checked_index.add(k_doi)

            tmp.w_tmpfile(
                "part_"+archive_name.replace(".zip", "")+".json", file_dois)

    out.w_outfile("res.json", index_dois)


# always specify the COCI dum path with the flag "--coci"
parser = argparse.ArgumentParser(description='Process the dump of COCI')
parser.add_argument('--coci', type=str, required=True,
                    help='path to the dir of the COCI dump (inner files all zipped)')
parser.add_argument('--getentries', type=str, required=False,
                    help='path to the CSV file containing a list of DOIs')
args = parser.parse_args()

if args.getentries:
    f_input = args.getentries.split("/")[-1]
    getentries(args.coci, args.getentries, Log("getentries_"+f_input),
               Tmp("getentries_"+f_input), Out("getentries_"+f_input))

print("done!")
# running example:
# nohup python proc_coci.py --coci /srv/data/coci/17 --getentries data/all_aabc_and_rsbmt.txt &
