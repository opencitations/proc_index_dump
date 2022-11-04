import argparse
import multiprocessing
import pdump

# always specify the COCI dum path with the flag "--coci"
parser = argparse.ArgumentParser(description='Process the dump of COCI')
parser.add_argument('--dump', type=str, required=True,
                    help='path to the dir of the COCI/Index dump (inner files all zipped)')

parser.add_argument('--selection', type=str, required=False, default="*",
                    help='the criteria to apply for the items selection from the INDEX dump. Options are: "*" | a CSV file containing a list of DOIs/OCIs | a file defining a set of rules')

parser.add_argument('--np', type=int, required=False, default=1,
                    help='number of processes')
parser.add_argument('--job', type=int, required=False, default=0,
                    help='the identifier of the job (used for backup reasons)')

parser.add_argument('--operation', type=str, required=False, default="cits_refs",
                    help='the operation to be done, available: cits_refs | citation_count')
args = parser.parse_args()

# init the dump
indexdump = pdump.IndexDump(args.dump, args.np, args.job)

# process the dump
indexdump.process(args.selection, args.operation)


print("All done!")
# running example:
# python cmd_pdump.py --dump /srv/data/coci/17 --selection duplicates.csv --operation cits_refs --np 8 --job 1
