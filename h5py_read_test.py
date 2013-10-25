from __future__ import division
from ipdb import set_trace as dbg

import h5py
import time as t
import numpy as np
import sys
import os
from os.path import join

from itertools import izip

def print_result(start_time, inp, out, read):
    print "\t#{0} Read#".format(read)
    print "\tRead {1} bytes in {0:.2f}sec.".format(t.time() - start_time, sys.getsizeof(inp) + sys.getsizeof(out))
    start_time = t.time()
    print "\tSummed to {0} in {1:.2f}sec.\n".format(np.sum(inp) + np.sum(out), t.time() - start_time)


def read_test_zero_group_one_element_mnist(filename, index):
    print filename

    with h5py.File(filename, mode='r') as f:
        start_time = t.time()
        inp = f['raw']['input'][()]
        out = f['raw']['output'][()]
        print_result(start_time, inp, out, "In RAM")

    with h5py.File(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f['raw']['input'], f['raw']['output']):
            inp.append(i)
            out.append(o)
        print_result(start_time, inp, out, "Seq")

    with h5py.File(filename, mode='r') as f:
        inp = []
        out = []
        start_time = t.time()
        A = f['raw']['input']
        B = f['raw']['output']
        for i in index:
            inp.append(A[i])
            out.append(B[i])
        print_result(start_time, inp, out, "Random")


def read_test_one_group_multiple_element_mnist(filename, index):
    print filename

    with h5py.File(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f['raw']['input'].itervalues(), f['raw']['output'].itervalues()):
            dbg()
            inp.append(i[()])
            out.append(o[()])
        print_result(start_time, inp, out, "Seq - Iter")

    with h5py.File(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        A = f['raw']['input']
        B = f['raw']['output']
        for i in index:
            inp.append(A["m_{0}_0".format(i)][()])
            out.append(B["t_{0}_0".format(i)][()])
        print_result(start_time, inp, out, "Random")


def read_test_mutiple_group_one_element_mnist(filename, index):
    print filename

    with h5py.File(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i in index:
            inp.append(f["raw/input/example{0}/m_{0}_0".format(i)][()])
            out.append(f["raw/output/target{0}/t_{0}_0".format(i)][()])
        print_result(start_time, inp, out, "Seq")
    
    with h5py.File(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f['raw']['input'].itervalues(), f['raw']['output'].itervalues()):
            for i2, o2 in izip(i.itervalues(), o.itervalues()):
                inp.append(i2[()])
                out.append(o2[()])
        print_result(start_time, inp, out, "Seq - Iter")

    with h5py.File(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        A = f['raw']['input']
        B = f['raw']['output']
        for i in index:
            inp.append(A["example{0}/m_{0}_0".format(i)][()])
            out.append(B["target{0}/t_{0}_0".format(i)][()])
        print_result(start_time, inp, out, "Random")

    with h5py.File(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i in index:
            inp.append(f["raw/input/example{0}/m_{0}_0".format(i)][()])
            out.append(f["raw/output/target{0}/t_{0}_0".format(i)][()])
        print_result(start_time, inp, out, "Random - Full naming")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else '.'

    dataset_size = 70000
    index = np.random.permutation(dataset_size)

    already_completed = []
    if os.path.isfile(join(path, 'done.txt')):
        already_completed = open(join(path, 'done.txt')).read().split()

    completed_file = open(join(path, 'done.txt'), 'a')

    filenames = [f for f in os.listdir(path) if f.endswith('.h5')]
    filenames = [f for f in filenames if not f in already_completed]
    filenames = [f for f in filenames if not 'zlib' in f]
    filenames = [f for f in filenames if not 'bzip2' in f]
    filenames = [f for f in filenames if not 'blosc' in f]

    for f in filenames:
        if 'zero_group_one_element' in f:
            read_test_zero_group_one_element_mnist(f, index)
        elif 'one_group_multiple_element' in f:
            read_test_one_group_multiple_element_mnist(f, index)
        elif 'mutiple_group_one_element' in f:
            read_test_mutiple_group_one_element_mnist(f, index)

        completed_file.write(f + "\n")