from __future__ import division
import tables
import time as t
import numpy as np
import sys


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
        inp = []
        out = []
        for i in f.root.raw.input:
            inp.append(i)
        for i in f.root.raw.output:
            out.append(i)
        print_result(start_time, inp, out, "Seq")

        inp = []
        out = []
        start_time = t.time()
        for i in index:
            inp.append(f.root.raw.input[i])
            out.append(f.root.raw.output[i])
        print_result(start_time, inp, out, "Random")


def read_test_mutiple_group_one_element_mnist(index):
    filename = "F_mutiple_group_one_element_mnist.h5"
    print filename
    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i in f.list_nodes(f.root.raw.input):
            inp += i
        for i in f.list_nodes(f.root.raw.output):
            out += i
        print_result(start_time, inp, out, "Seq")

        start_time = t.time()
        inp = []
        out = []
        for i in f.iter_nodes(f.root.raw.input):
            inp += i
        for i in f.iter_nodes(f.root.raw.output):
            out += i
        print_result(start_time, inp, out, "SeqIter")

        start_time = t.time()
        inp = []
        out = []
        for i in index:
            inp += f.get_node(f.root.raw.input, "example{0}".format(i))
            out += f.get_node(f.root.raw.output, "target{0}".format(i))
        print_result(start_time, inp, out, "Random")


def read_test_one_group_multiple_element_mnist(index):
    filename = "F_one_group_multiple_element_mnist.h5"
    print filename
    with tables.open_file(filename, mode='r') as f:

        start_time = t.time()
        inp = []
        out = []
        for i in f.list_nodes(f.root.raw.input):
            inp += i
        for i in f.list_nodes(f.root.raw.output):
            out += i
        print_result(start_time, inp, out, "Seq")

        start_time = t.time()
        inp = []
        out = []
        for i in f.iter_nodes(f.root.raw.input):
            inp += i
        for i in f.iter_nodes(f.root.raw.output):
            out += i
        print_result(start_time, inp, out, "SeqIter")

        start_time = t.time()
        inp = []
        out = []
        for i in index:
            inp += f.get_node(f.root.raw.input, "m_{0}_0".format(i))[0]
            out += f.get_node(f.root.raw.output, "t_{0}_0".format(i))[0]
        print_result(start_time, inp, out, "Random")


if __name__ == "__main__":
    dataset_size = 70000
    index = np.random.permutation(dataset_size)

    #read_test_zero_group_one_element_mnist(index)
    #read_test_mutiple_group_one_element_mnist(index)
    read_test_one_group_multiple_element_mnist(index)
