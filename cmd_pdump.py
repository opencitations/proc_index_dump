import argparse
import multiprocessing
import pdump

# always specify the COCI dum path with the flag "--coci"
parser = argparse.ArgumentParser(description='Process the dump of COCI')
parser.add_argument('--dump', type=str, required=True,
                    help='path to the dir of the COCI/Index dump (inner files all zipped)')
parser.add_argument('--getbylist', type=str, required=False,
                    help='path to the CSV file containing a list of DOIs')
parser.add_argument('--getbyrule', type=str, required=False,
                    help='a string which defines the rule a citation should respect in order to be retrieved')
parser.add_argument('--np', type=str, required=False, default="1",
                    help='number of processes')
args = parser.parse_args()


indexdump = pdump.IndexDump(args.dump, args.np)
# call corresponding method
if args.getbylist:
    indexdump.getbylist(args.getbylist)


print("done!")
# running example:
# nohup python cmd_pdump.py --dump /srv/data/coci/17 --getbylist ../data/all_aabc_and_rsbmt.txt -np 1&
