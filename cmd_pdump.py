import argparse
import multiprocessing
import pdump

# always specify the COCI dum path with the flag "--coci"
parser = argparse.ArgumentParser(description='Process the dump of COCI')
parser.add_argument('--dump', type=str, required=True,
                    help='path to the dir of the COCI/Index dump (inner files all zipped)')
parser.add_argument('--getbylist', type=str, required=False,
                    help='the path to the CSV file containing a list of DOIs/OCIs')
parser.add_argument('--getbyrule', type=str, required=False,
                    help='a string which defines the rule a citation should respect in order to be retrieved')
parser.add_argument('--np', type=int, required=False, default=1,
                    help='number of processes')
parser.add_argument('--job', type=int, required=False, default=0,
                    help='the identifier of the job (used for backup reasons)')
args = parser.parse_args()


indexdump = pdump.IndexDump(args.dump, args.np, args.job)
# call corresponding method

# --getbylist: must contain also
# <oci|doi;FILE>
if args.getbylist:
    indexdump.getbylist(args.getbylist)


print("All done!")
# running example:
# python cmd_pdump.py --dump /srv/data/coci/17 --getbylist duplicates.csv --np 8 --job 1
