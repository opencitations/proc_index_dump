import csv
import json
from zipfile import ZipFile
import os
import datetime
import io
import multiprocessing


class Log:
    def __init__(self, job_name, pid):
        self.dir = ".log"
        self.pid = str(pid)

        if not os.path.exists(self.dir):
            os.makedirs(self.dir)

        jobdir = self.dir+"/"+job_name
        if not os.path.exists(jobdir):
            os.makedirs(jobdir)

        #time = str(datetime.datetime.now()).replace(" ", "_")
        #self.flog = self.dir+"/"+pname+"_"+time+".log"
        self.flog = jobdir+"/"+pid+".log"
        if not os.path.exists(self.flog):
            with open(self.flog, 'w') as f:
                f.write("time,msg")

    def w_log(self, msg):
        with open(self.flog, 'a') as f:
            time = str(datetime.datetime.now())
            f.write(time+","+msg+"\n")


class Tmp:
    def __init__(self, job_name, pid):
        self.dir = ".tmp"
        self.pid = str(pid)

        if not os.path.exists(self.dir):
            os.makedirs(self.dir)

        jobdir = self.dir+"/"+job_name
        if not os.path.exists(jobdir):
            os.makedirs(jobdir)

        self.procdir = jobdir+"/"+pid
        if not os.path.exists(self.procdir):
            os.makedirs(self.procdir)

    def w_tmpfile(self, f_name, content):
        if f_name.endswith(".json"):
            fdest = self.procdir+"/"+f_name
            with open(fdest, 'w') as file:
                json.dump(content, file)


class Out:
    def __init__(self, job_name):
        self.dir = "out_"+job_name
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)

    def w_outfile(self, f_name, content):
        if f_name.endswith(".json"):
            fdest = self.dir+"/"+f_name
            with open(fdest, 'w') as file:
                json.dump(content, file)


def read_csv_to_ldict(fpath):
    reader = csv.DictReader(io.TextIOWrapper(fpath))
    return list(reader)


class IndexDump():
    def __init__(self, dump, np=1):
        self.np = np
        self.index_dir = dump
        self.log = None
        self.tmp = None
        self.out = None

    def getbylist(self, fcsv):

        def prun(dois_csv, pid, log, tmp, out):

            # Init the output
            index_dois = dict()
            with open(dois_csv, 'r') as data:
                for line in csv.reader(data):
                    doi_val = line[0]
                    #normalize doi value
                    doi_val = doi_val.strip().lower()
                    index_dois[doi_val] = dict()
                    index_dois[doi_val]["citations"] = []
                    index_dois[doi_val]["references"] = []

            # Handle backup
            # ---
            checked_index = set()
            bkup_f_index = set()
            for bkup_f in os.listdir(tmp.procdir):
                # bkup_f = part_2020-01-13T19_31_19_1-4.json
                if bkup_f.endswith("json") and bkup_f.startswith("part_"):
                    log.w_log("BACKUP_FILE: " + str(bkup_f))
                    zip_f_name = bkup_f.split(
                        ".")[0].replace("part_", "")+".zip"
                    bkup_f_index.add(zip_f_name)
                    for k, v in json.load(open(tmp.procdir+"/"+bkup_f)).items():
                        index_dois[k]["citations"] += v["citations"]
                        index_dois[k]["references"] += v["references"]
                        if len(v["citations"]) > 0 or len(v["references"]) > 0:
                            checked_index.add(k)

                    log.w_log("BACKUP_DOIS: " + str(len(checked_index)))

            # read the dump of COCI
            log.w_log("RUNNING_PROC")
            for archive_name in os.listdir(self.index_dir):
                if archive_name.endswith("zip") and archive_name not in bkup_f_index:
                    archive_path = os.path.join(self.index_dir, archive_name)
                    log.w_log("PROCESSING_FILE: "+archive_name)
                    tmp_index = dict()
                    with ZipFile(archive_path) as archive:
                        for csv_name in archive.namelist():
                            with archive.open(csv_name) as csv_file:
                                l_cits = read_csv_to_ldict(csv_file)
                                # df_data = read_csv(csv_file, encoding='utf-8')

                                for item in l_cits:

                                    # check if "citing" or "cited" value is in index_dois
                                    if item["citing"] in index_dois:
                                        index_dois[item["citing"]
                                                   ]["references"].append(item)
                                        if item["citing"] not in tmp_index:
                                            tmp_index[item["citing"]] = {
                                                "citations": [], "references": []}
                                            log.w_log(
                                                "DOI_FOUND: "+item["citing"])
                                        tmp_index[item["citing"]
                                                  ]["references"].append(item)

                                    if item["cited"] in index_dois:
                                        index_dois[item["cited"]
                                                   ]["citations"].append(item)
                                        if item["cited"] not in tmp_index:
                                            tmp_index[item["cited"]] = {
                                                "citations": [], "references": []}
                                            log.w_log(
                                                "DOI_FOUND: "+item["cited"])
                                        tmp_index[item["cited"]
                                                  ]["citations"].append(item)

                    tmp.w_tmpfile(
                        "part_"+archive_name.replace(".zip", "")+".json", tmp_index)

            fout = str(pid)+"_res.json"
            out.w_outfile(fout, index_dois)
            return fout

        pindex = dict()
        k_job = fcsv.split("/")[-1]

        for pid in range(0, self.np):
            self.log = Log("getbylist_"+k_job, pid)
            self.tmp = Tmp("getbylist_"+k_job, pid)
            self.out = Out("getbylist_"+k_job)
            pindex[pid] = multiprocessing.Process(
                target=prun, args=(fcsv, self.log, self.tmp, self.out,))
            pindex[pid].start()

        for pid in range(0, self.np):
            pindex[pid].join()
