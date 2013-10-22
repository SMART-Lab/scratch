from __future__ import division
#from ipdb import set_trace as dbg

import tables
import time as t
import numpy as np
import sys

from itertools import izip


def print_result(start_time, inp, out, read):
    print "\t#{0} Read#".format(read)
    print "\tRead {1} bytes in {0:.2f}sec.".format(t.time() - start_time, sys.getsizeof(inp) + sys.getsizeof(out))
    start_time = t.time()
    print "\tSummed to {0} in {1:.2f}sec.\n".format(np.sum(inp) + np.sum(out), t.time() - start_time)


def read_test_zero_group_one_element_mnist(index):
    filename = "F_zero_group_one_element_mnist.h5"
    print filename
    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = f.root.raw.input.read()
        out = f.root.raw.output.read()
        print_result(start_time, inp, out, "InRAM")

    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = f.root.raw.input
        out = f.root.raw.output
        print_result(start_time, inp, out, "InRAM - Lazy")

    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f.root.raw.input, f.root.raw.output):
            inp.append(i)
            out.append(o)
        print_result(start_time, inp, out, "Seq")

    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f.root.raw.input.iterrows(), f.root.raw.output.iterrows()):
            inp.append(i)
            out.append(o)
        print_result(start_time, inp, out, "Seq - Iterrows")
    
    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f.root.raw.input, f.root.raw.output):
            inp.append(i)
            out.append(o)
        print_result(start_time, inp, out, "Seq")

    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i in index:
            inp.append(f.root.raw.input[i])
            out.append(f.root.raw.output[i])
        print_result(start_time, inp, out, "Random")

    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        A = f.root.raw.input
        B = f.root.raw.output
        for i in index:
            inp.append(A[i])
            out.append(B[i])
        print_result(start_time, inp, out, "Random - w/o natural naming")


def read_test_one_group_multiple_element_mnist(index):
    filename = "F_one_group_multiple_element_mnist.h5"
    print filename
    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f.iter_nodes(f.root.raw.input), f.iter_nodes(f.root.raw.output)):
            inp.append(i.read())
            out.append(o.read())
        print_result(start_time, inp, out, "SeqIter")
    
    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f.iter_nodes(f.root.raw.input), f.iter_nodes(f.root.raw.output)):
            inp.append(i)
            out.append(o)
        print_result(start_time, inp, out, "SeqIter - Lazy")

    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f.walk_nodes(f.root.raw.input, 'Leaf'), f.walk_nodes(f.root.raw.output, 'Leaf')):
            inp.append(i.read())
            out.append(o.read())
        print_result(start_time, inp, out, "SeqWalk")

    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f.walk_nodes(f.root.raw.input, 'Leaf'), f.walk_nodes(f.root.raw.output, 'Leaf')):
            inp.append(i)
            out.append(o)
        print_result(start_time, inp, out, "SeqWalk - Lazy")

    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i in index:
            inp.append(f.get_node(f.root.raw.input, "m_{0}_0".format(i)).read())
            out.append(f.get_node(f.root.raw.output, "t_{0}_0".format(i)).read())
        print_result(start_time, inp, out, "Random")

    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i in index:
            inp.append(f.get_node(f.root.raw.input, "m_{0}_0".format(i)))
            out.append(f.get_node(f.root.raw.output, "t_{0}_0".format(i)))
        print_result(start_time, inp, out, "Random - Lazy")

    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        A = f.root.raw.input
        B = f.root.raw.output
        for i in index:
            inp.append(f.get_node(A, "m_{0}_0".format(i)).read())
            out.append(f.get_node(B, "t_{0}_0".format(i)).read())
        print_result(start_time, inp, out, "Random - w/o natural naming")

    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        A = f.root.raw.input
        B = f.root.raw.output
        for i in index:
            inp.append(f.get_node(A, "m_{0}_0".format(i)))
            out.append(f.get_node(B, "t_{0}_0".format(i)))
        print_result(start_time, inp, out, "Random - w/o natural naming - Lazy")


def read_test_mutiple_group_one_element_mnist(index):
    filename = "F_mutiple_group_one_element_mnist.h5"
    print filename
    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f.iter_nodes(f.root.raw.input), f.iter_nodes(f.root.raw.output)):
            inp.append(f.list_nodes(i)[0].read())
            out.append(f.list_nodes(o)[0].read())
        print_result(start_time, inp, out, "SeqIter")
        
    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f.iter_nodes(f.root.raw.input), f.iter_nodes(f.root.raw.output)):
            inp.append(f.list_nodes(i)[0])
            out.append(f.list_nodes(o)[0])
        print_result(start_time, inp, out, "SeqIter - Lazy")

    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f.walk_nodes(f.root.raw.input, 'Leaf'), f.walk_nodes(f.root.raw.output, 'Leaf')):
            inp.append(i.read())
            out.append(o.read())
        print_result(start_time, inp, out, "SeqWalk")

    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i, o in izip(f.walk_nodes(f.root.raw.input, 'Leaf'), f.walk_nodes(f.root.raw.output, 'Leaf')):
            inp.append(i)
            out.append(o)
        print_result(start_time, inp, out, "SeqWalk - Lazy")

    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i in index:
            inp.append(f.get_node(f.root.raw.input, "example{0}/m_{0}_0".format(i)).read())
            out.append(f.get_node(f.root.raw.output, "target{0}/t_{0}_0".format(i)).read())
        print_result(start_time, inp, out, "Random")

    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i in index:
            inp.append(f.get_node(f.root.raw.input, "example{0}/m_{0}_0".format(i)))
            out.append(f.get_node(f.root.raw.output, "target{0}/t_{0}_0".format(i)))
        print_result(start_time, inp, out, "Random - Lazy")

    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        A = f.root.raw.input
        B = f.root.raw.output
        for i in index:
            inp.append(f.get_node(A, "example{0}/m_{0}_0".format(i)).read())
            out.append(f.get_node(B, "target{0}/t_{0}_0".format(i)).read())
        print_result(start_time, inp, out, "Random - w/o natural naming - Lazy")

    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        A = f.root.raw.input
        B = f.root.raw.output
        for i in index:
            inp.append(f.get_node(A, "example{0}/m_{0}_0".format(i)))
            out.append(f.get_node(B, "target{0}/t_{0}_0".format(i)))
        print_result(start_time, inp, out, "Random - w/o natural naming - Lazy")


if __name__ == "__main__":
    dataset_size = 70000
    index = np.random.permutation(dataset_size)

    read_test_zero_group_one_element_mnist(index)
    read_test_one_group_multiple_element_mnist(index)
    read_test_mutiple_group_one_element_mnist(index)
