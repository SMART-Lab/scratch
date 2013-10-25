from __future__ import division
from ipdb import set_trace as dbg

import tables
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


def read_test_zero_group_one_element_mnist(filename, index, filters=None):
    print filename
    with tables.open_file(filename, mode='r', filters=filters) as f:
        start_time = t.time()
        inp = f.root.raw.input.read()
        out = f.root.raw.output.read()
        print_result(start_time, inp, out, "In RAM")

    with tables.open_file(filename, mode='r', filters=filters) as f:
        start_time = t.time()
        inp = []
        out = []

        for i, o in izip(f.root.raw.input, f.root.raw.output):
            inp.append(i)
            out.append(o)
        print_result(start_time, inp, out, "Seq")

    with tables.open_file(filename, mode='r', filters=filters) as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f.root.raw.input.iterrows(), f.root.raw.output.iterrows()):
            inp.append(i)
            out.append(o)
        print_result(start_time, inp, out, "Seq - Iter")

    with tables.open_file(filename, mode='r', filters=filters) as f:
        start_time = t.time()
        inp = []
        out = []
        A = f.root.raw.input
        B = f.root.raw.output
        for i in index:
            inp.append(A[i])
            out.append(B[i])

        print_result(start_time, inp, out, "Random")


def read_test_one_group_multiple_element_mnist(filename, index, filters=None):
    print filename
    # dbg()
    # start_time = t.time()
    # with tables.open_file(filename, mode='r', filters=filters, PYTABLES_SYS_ATTRS=False) as f:
    #     print t.time() - start_time
    #     dbg()
    #     inp = []
    #     out = []
    #     for i in f.root.raw.input:
    #         dbg()
    #         inp.append(i.read())

    #     dbg()
    #     print_result(start_time, inp, out, "Seq - Iter")
    # dbg()

    print filename
    dbg()
    start_time = t.time()
    with tables.open_file(filename, mode='r', filters=filters) as f:
        print t.time() - start_time
        inp = []
        out = []
        for i, o in izip(f.iter_nodes(f.root.raw.input), f.iter_nodes(f.root.raw.output)):
            inp.append(i.read())
            out.append(o.read())

        print_result(start_time, inp, out, "Seq - Iter")

    # with tables.open_file(filename, mode='r') as f:
    #     start_time = t.time()
    #     inp = []
    #     out = []
    #     for i, o in izip(f.iter_nodes(f.root.raw.input), f.iter_nodes(f.root.raw.output)):
    #         inp.append(i)
    #         out.append(o)
    #     print_result(start_time, inp, out, "SeqIter - Lazy")

    start_time = t.time()
    with tables.open_file(filename, mode='r', filters=filters) as f:
        print t.time() - start_time
        inp = []
        out = []
        for i, o in izip(f.walk_nodes(f.root.raw.input, 'Leaf'), f.walk_nodes(f.root.raw.output, 'Leaf')):
            inp.append(i.read())
            out.append(o.read())
        print_result(start_time, inp, out, "Seq - Walk")

    # with tables.open_file(filename, mode='r') as f:
    #     start_time = t.time()
    #     inp = []
    #     out = []
    #     for i in index:
    #         inp.append(f.get_node(f.root.raw.input, "m_{0}_0".format(i)).read())
    #         out.append(f.get_node(f.root.raw.output, "t_{0}_0".format(i)).read())
    #     print_result(start_time, inp, out, "Random - w/ natural naming")

    start_time = t.time()
    with tables.open_file(filename, mode='r', filters=filters) as f:
        print t.time() - start_time
        inp = []
        out = []
        A = f.root.raw.input
        B = f.root.raw.output
        for i in index:
            inp.append(f.get_node(A, "m_{0}_0".format(i)).read())
            out.append(f.get_node(B, "t_{0}_0".format(i)).read())
        print_result(start_time, inp, out, "Random")


def read_test_mutiple_group_one_element_mnist(filename, index, filters=None):
    print filename
    with tables.open_file(filename, mode='r', filters=filters) as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f.root.raw.input, f.root.raw.output):
            for i2, o2 in izip(i, o):
                inp.append(i2.read())
                out.append(o2.read())
        print_result(start_time, inp, out, "Seq")

    with tables.open_file(filename, mode='r', filters=filters) as f:
        start_time = t.time()
        inp = []
        out = []
        A = f.root.raw.input
        B = f.root.raw.output
        for i in xrange(A._v_nchildren):
            inp.append(f.get_node(A, "example{0}/m_{0}_0".format(i)).read())
            out.append(f.get_node(B, "target{0}/t_{0}_0".format(i)).read())
        print_result(start_time, inp, out, "Seq - Naming")

    with tables.open_file(filename, mode='r', filters=filters) as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f.iter_nodes(f.root.raw.input), f.iter_nodes(f.root.raw.output)):
            for i2, o2 in izip(f.iter_nodes(i), f.iter_nodes(o)):
                inp.append(i2.read())
                out.append(o2.read())
        print_result(start_time, inp, out, "Seq - Iter")

    with tables.open_file(filename, mode='r', filters=filters) as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f.walk_nodes(f.root.raw.input, 'Leaf'), f.walk_nodes(f.root.raw.output, 'Leaf')):
            inp.append(i.read())
            out.append(o.read())
        print_result(start_time, inp, out, "Seq - Walk")

    with tables.open_file(filename, mode='r', filters=filters) as f:
        start_time = t.time()
        inp = []
        out = []
        A = f.root.raw.input
        B = f.root.raw.output
        for i in index:
            inp.append(f.get_node(A, "example{0}/m_{0}_0".format(i)).read())
            out.append(f.get_node(B, "target{0}/t_{0}_0".format(i)).read())
        print_result(start_time, inp, out, "Random")

    with tables.open_file(filename, mode='r', filters=filters) as f:
        start_time = t.time()
        inp = []
        out = []
        for i in index:
            inp.append(f.get_node("/raw/input/example{0}/m_{0}_0".format(i)).read())
            out.append(f.get_node("/raw/output/target{0}/t_{0}_0".format(i)).read())
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
    filenames = [f for f in filenames if not 'gzip' in f]
    filenames = [f for f in filenames if not 'lzf' in f]
    #filenames = [f for f in filenames if not 'zlib' in f]
    #filenames = [f for f in filenames if not 'blosc' in f]
    #filenames = [f for f in filenames if not 'bzip2' in f]

    for f in filenames:
        filters = None
        if 'blosc' in f:
            filters = tables.filters.Filters(complevel=9, complib='blosc')
        elif 'zlib' in f:
            filters = tables.filters.Filters(complevel=9, complib='zlib')
        elif 'bzip2' in f:
            filters = tables.filters.Filters(complevel=9, complib='bzip2')

        if 'zero_group_one_element' in f:
            #read_test_zero_group_one_element_mnist(f, index, filters)
            pass
        elif 'one_group_multiple_element' in f:
            read_test_one_group_multiple_element_mnist(f, index, filters)
        elif 'mutiple_group_one_element' in f:
            #read_test_mutiple_group_one_element_mnist(f, index, filters)
            pass

        completed_file.write(f + "\n")
