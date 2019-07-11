#!/usr/bin/env python
import os
import subprocess
import sys
import time
import math
import itertools
from threading import Thread
xrange = range

def run_seg(i, prev_thread, tuplist, typ, p, r, d, s, outfile):
    # TODO: Change this
    cmd = '~/monet/src/pcl/rgselect {} {} {} -p {} -r {} -d {} -s {}'
    collated_segments = ''
    for bucket, filename in tuplist:
        cmd_fmt = cmd.format(filename, typ, bucket, p, r, d, s)
        collated_segments += subprocess.check_output(cmd_fmt, shell=True)
        print ('[+] {} complete.'.format(filename))
    print ('[+] thread {} waiting for previous'.format(i))
    if prev_thread is not None:
        prev_thread.join()
    with open(outfile, 'a+') as f:
        f.write(collated_segments)
    print ('[+] thread {} done'.format(i))

def split_date(datefiles, num_threads):
    size = int(math.ceil(10 // num_threads))
    start = 1
    for i in xrange(num_threads):
        s = [(j, os.path.join(datefiles, 'b_{:04}.pcd'.format(j)))
             for j in xrange(start, min(11, start + size))]
        yield s
        start += size

def run_threaded_seg(num_threads, datefiles, typ, p, r, d, s, outfile):
    start_time = time.time()
    thread_list = []
    prev_thread = None
    for i, tuplist in enumerate(split_date(datefiles, num_threads)):
        t = Thread(target=run_seg, args=(i, prev_thread, tuplist, typ, p, r, d, s, outfile))
        t.start()
        prev_thread = t
        thread_list.append(t)
    for t in thread_list:
        t.join()
    end_time = time.time()
    print ('[+] Time elapsed (s): {}'.format(end_time - start_time))

if __name__ == '__main__':
    if sys.argv[1] == 'tuning':
        num_threads = int(sys.argv[2])
        datefiles = sys.argv[3]
        typ = int(sys.argv[4])
        outdir = sys.argv[5]
        ps = [2, 4, 6, 8]
        rs = [2, 4, 6, 8]
        ds = [2]
        ss = [10, 15, 20, 25, 30, 35, 40, 50]

        try:
            os.mkdir(outdir)
        except OSError:
            pass

        for p, r, d, s in itertools.product(ps, rs, ds, ss):
            outfile = os.path.join(outdir, 'regions_{}_{}-{}-{}-{}_{}'.format(typ, p, r, d, s, os.path.basename(datefiles)))
            run_threaded_seg(num_threads, datefiles, typ, p, r, d, s, outfile)
    elif sys.argv[1] == 'single':
        num_threads = int(sys.argv[2])
        datefiles = sys.argv[3]
        typ = int(sys.argv[4])
        p = int(sys.argv[5])
        r = int(sys.argv[6])
        d = int(sys.argv[7])
        s = int(sys.argv[8])
        outdir = sys.argv[9]

        try:
            os.mkdir(outdir)
        except OSError:
            pass

        outfile = os.path.join(outdir, 'regions_original_{}_{}-{}-{}-{}_{}'.format(typ, p, r, d, s, os.path.basename(datefiles)))
        run_threaded_seg(num_threads, datefiles, typ, p, r, d, s, outfile)
