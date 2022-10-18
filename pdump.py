import csv
import json
from zipfile import ZipFile
import os
import datetime
import io
import multiprocessing
import numpy as np
import re
import configparser


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
                f.write("time,msg\n")

    def w_log(self, msg):
        with open(self.flog, 'a') as f:
            time = str(datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S"))
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

    def w_outfile(self, f_name, content):

        if not os.path.exists(self.dir):
            os.makedirs(self.dir)

        if f_name.endswith(".json"):
            fdest = self.dir+"/"+f_name
            with open(fdest, 'w') as file:
                json.dump(content, file)

        return fdest


class ParallelEnv:

    def __init__(self, np):
        self.np = np

    def getpwork(self, in_dir, pid):
        # set the part of data each process needs to analyse
        zipfiles = []
        for archive_name in os.listdir(self.index_dir):
            if archive_name.endswith("zip"):
                zipfiles.append(archive_name)
        zipfiles_splits = np.array_split(zipfiles, self.np)
        if pid >= len(zipfiles_splits):
            return ([],"Too much processes: number of process should not exceed the number of files. This process will not start")

        return (zipfiles_splits[pid],"FILES_TO_PROCESS: "+"; ".join(zipfiles_splits[pid]))


def csv_to_ldict(fpath):
    reader = csv.DictReader(io.TextIOWrapper(fpath))
    return list(reader)


class IndexDump():
    def __init__(self, dump, np=1, job=0):
        self.np = np
        self.job = "j"+str(job)
        self.index_dir = dump
        self.log = None
        self.tmp = None
        self.out = None

    def getbylist(self, fcsv):

        def p_start(dois_csv, log, tmp, out, pid, p_files):

            # Init the output
            field = "doi"
            index_res = dict()
            with open(dois_csv, 'r') as data:
                for line in csv.reader(data):
                    #normalize value in list doi|oci
                    k_val = line[0].strip().lower()
                    index_res[k_val] = dict()
                    # check if it is a list of OCIs
                    if "/" not in k_val and "-" in k_val:
                        field = "oci"
                    if field == "doi":
                        index_res[k_val]["citations"] = []
                        index_res[k_val]["references"] = []
                    elif field == "oci":
                        index_res[k_val] = []

            # Handle backup
            # ---
            bkup_f_index = set()
            for bkup_f in os.listdir(tmp.procdir):
                # bkup_f = part_2020-01-13T19_31_19_1-4.json
                if bkup_f.endswith("json") and bkup_f.startswith("part_"):
                    log.w_log("BACKUP_FILE: " + str(bkup_f))
                    zip_f_name = bkup_f.split(
                        ".")[0].replace("part_", "")+".zip"
                    bkup_f_index.add(zip_f_name)
                    for k, v in json.load(open(tmp.procdir+"/"+bkup_f)).items():
                        if field == "doi":
                            index_res[k]["citations"] += v["citations"]
                            index_res[k]["references"] += v["references"]
                        elif field == "oci":
                            index_res[k] += v

            # read the dump
            log.w_log("RUNNING_PROC")
            for archive_name in p_files:
                if archive_name not in bkup_f_index:
                    archive_path = os.path.join(self.index_dir, archive_name)
                    log.w_log("PROCESSING_FILE: "+archive_name)
                    tmp_index = dict()
                    with ZipFile(archive_path) as archive:
                        for csv_name in archive.namelist():
                            with archive.open(csv_name) as csv_file:
                                l_cits = csv_to_ldict(csv_file)
                                # df_data = read_csv(csv_file, encoding='utf-8')

                                for item in l_cits:

                                    if field == "doi":
                                        # check if "citing"
                                        if item["citing"] in index_res:
                                            index_res[item["citing"]
                                                      ]["references"].append(item)
                                            if item["citing"] not in tmp_index:
                                                tmp_index[item["citing"]] = {
                                                    "citations": [], "references": []}
                                                log.w_log(
                                                    "DOI_FOUND: "+item["citing"]+ "; FOUND_IN: "+str(csv_name))
                                            tmp_index[item["citing"]
                                                      ]["references"].append(item)
                                        # or "cited" value is in index_res
                                        if item["cited"] in index_res:
                                            index_res[item["cited"]
                                                      ]["citations"].append(item)
                                            if item["cited"] not in tmp_index:
                                                tmp_index[item["cited"]] = {
                                                    "citations": [], "references": []}
                                                log.w_log(
                                                    "DOI_FOUND: "+item["cited"]+ "; FOUND_IN: "+str(csv_name))
                                            tmp_index[item["cited"]
                                                      ]["citations"].append(item)

                                    elif field == "oci":
                                        if item["oci"] in index_res:
                                            index_res[item["oci"]].append(item)
                                            if item["oci"] not in tmp_index:
                                                tmp_index[item["oci"]] = []
                                                log.w_log("OCI_FOUND: "+item["oci"]+ "; FOUND_IN: "+str(csv_name))
                                            tmp_index[item["oci"]].append(item)

                    tmp.w_tmpfile(
                        "part_"+archive_name.replace(".zip", "")+".json", tmp_index)

            #Done
            log.w_log("PROC_DONE")

            fout = str(pid)+"_res.json"
            out.w_outfile(fout, index_res)

        def p_join(out):
            join_res = dict()
            for res_f in os.listdir(out.dir):
                if res_f.endswith("json"):
                    with open(os.path.join(out.dir, res_f), 'r') as j_file:
                        # Join results
                        j_content = json.load(j_file)
                        for k,v in j_content.items():
                            if k not in join_res:
                                # check DOI or OCI
                                if "/" not in k and "-" in k:
                                    join_res[k] = []
                                else:
                                    join_res[k] = dict()
                                    join_res[k]["citations"] = []
                                    join_res[k]["references"] = []

                            if "/" not in k and "-" in k:
                                if v != None:
                                    join_res[k] += v
                            else:
                                join_res[k]["citations"] += v["citations"]
                                join_res[k]["references"] += v["references"]

            out.w_outfile("res.json", join_res)

        # Run processes and wait
        # ----------------------
        p_index = dict()
        self.out = Out(self.job)
        parallelenv = ParallelEnv(self.np)
        for pid in range(0, self.np):
            self.log = Log(self.job, str(pid))
            self.tmp = Tmp(self.job, str(pid))

            pwork = parallelenv.getpwork(self.index_dir, pid)[0]
            log.w_log(pwork[1])

            p_index[pid] = dict()
            p_index[pid] = multiprocessing.Process(
                target=p_start, args=(fcsv, self.log, self.tmp, self.out, pid, pwork[0], ))
            p_index[pid].start()

        for pid in range(0, self.np):
            p_index[pid].join()

        p_join(self.out)


    def getbyrule(self, rules):

        def p_start(frules, log, tmp, out, pid, p_files):

            # Init the output
            index_res = []

            # Handle backup
            # ---
            bkup_f_index = set()
            for bkup_f in os.listdir(tmp.procdir):
                # bkup_f = part_2020-01-13T19_31_19_1-4.json
                if bkup_f.endswith("json") and bkup_f.startswith("part_"):
                    log.w_log("BACKUP_FILE: " + str(bkup_f))
                    zip_f_name = bkup_f.split(
                        ".")[0].replace("part_", "")+".zip"
                    bkup_f_index.add(zip_f_name)
                    for row in json.load(open(tmp.procdir+"/"+bkup_f)):
                        index_res.append(row)

            #read the rules INI file
            rules = configparser.ConfigParser()
            rules.read(frules)
            index_rules = dict()
            for section in rules.sections():
                index_rules[section] = dict(rules.items(section))

            # read the dump
            log.w_log("RUNNING_PROC")
            for archive_name in p_files:
                if archive_name not in bkup_f_index:
                    archive_path = os.path.join(self.index_dir, archive_name)
                    log.w_log("PROCESSING_FILE: "+archive_name)
                    tmp_index = []
                    with ZipFile(archive_path) as archive:
                        for csv_name in archive.namelist():
                            with archive.open(csv_name) as csv_file:
                                for item in csv_to_ldict(csv_file):

                                    #check if one of the rules is TRUE
                                    rules_fine = False
                                    for k, conditions in index_rules.items():
                                        conditions_fine = True
                                        for c in conditions:
                                            # AND between conditions
                                            #conditions_fine = conditions_fine and [TODO]
                                        rules_fine = rules_fine or conditions_fine

                                    if rules_fine:
                                        #log.w_log("FINE_ENTRY: "+item["oci"])
                                        index_res.append(item)
                                        tmp_index.append(item)

                    tmp.w_tmpfile(
                        "part_"+archive_name.replace(".zip", "")+".json", tmp_index)
            #Done
            log.w_log("PROC_DONE")
            fout = str(pid)+"_res.json"
            out.w_outfile(fout, index_res)


        def p_join():

        # Run processes and wait
        # ----------------------
        p_index = dict()
        self.out = Out(self.job)
        parallelenv = ParallelEnv(self.np)
        for pid in range(0, self.np):
            self.log = Log(self.job, str(pid))
            self.tmp = Tmp(self.job, str(pid))

            # get the list of files this PID should handle
            pwork = parallelenv.getpwork(self.index_dir, pid)[0]
            log.w_log(pwork[1])

            p_index[pid] = dict()
            p_index[pid] = multiprocessing.Process(
                target=p_start, args=(rules, self.log, self.tmp, self.out, pid, pwork[0], ))
            p_index[pid].start()

        for pid in range(0, self.np):
            p_index[pid].join()

        p_join(self.out)
