from __future__ import division

from pdb import set_trace as dbg

import tables
import time as t
import numpy as np
import os

from glob import glob

rng = np.random.RandomState(1234)
nb_examples = 70000

def _write_file(write_method, f, filename, create, complib):
    raw = f.create_group('/', 'raw')
    start_time = t.time()
    write_method(nb_examples, f, raw, create)
    print "\tCompLib: {0: <3}\tTime: {1:.2f}sec\tSize: {2:.2f}Mb".format(complib, t.time() - start_time, os.path.getsize(filename) / 1024 / 1024)

def read_file(write_method, dataset_name):
    print write_method.__name__
    #filename = "{0}_{1}_{2}.h5".format(write_method.__name__, dataset_name)
    for complib in complibs:
        my_filters = tables.filters.Filters(complevel=9, complib=complib)
        filename = write_method.__name__ + '_' + dataset_name + '_' + complib + '_c9.h5'
        with tables.open_file(filename, mode='w', filters=my_filters, max_group_width=nb_examples) as f:
            _write_file(write_method, f, filename, f.create_carray, complib)
        os.remove(filename)

    filename = write_method.__name__ + '_' + dataset_name + '.h5'
    with tables.open_file(filename, mode='w', max_group_width=nb_examples) as f:
        _write_file(write_method, f, filename, f.create_array, "None")
    os.remove(filename)

# MAIN #####
if __name__ == "__main__":
    size = 70000
    index = range(size)
    rng.shuffle(index)

    # filename = "F_mutiple_group_one_element_mnist.h5"
    # print filename
    # with tables.open_file(filename, mode='r') as f:
    #     start_time = t.time()
    #     for i in index:
    #         inp = f.get_node(f.root.raw.input, "example{0}".format(i))
    #         out = f.get_node(f.root.raw.output, "target{0}".format(i))
    #     print "Time: {0:.2f}sec Random Read".format(t.time() - start_time)

    #     start_time = t.time()
    #     inp = f.list_nodes(f.root.raw.input)
    #     out = f.list_nodes(f.root.raw.output)
    #     print "Time: {0:.2f}sec Seq Read".format((t.time() - start_time))

    #     start_time = t.time()
    #     for i in f.iter_nodes(f.root.raw.input):
    #         inp = i
    #     for i in f.iter_nodes(f.root.raw.output):
    #         out = i
    #     print "Time: {0:.2f}sec SeqIter Read".format((t.time() - start_time))


    filename = "F_zero_group_one_element_mnist.h5"
    print filename
    import numpy as np
    import sys
    with tables.open_file(filename, mode='r') as f:
        start_time = t.time()
        inp = []
        out = []
        for i in f.root.raw.input:
            inp.append(i)

        for i in f.root.raw.output:
            out.append(i)

        #inp = np.asarray(f.root.raw.input)
        #out = np.asarray(f.root.raw.output)
        print "Time: {0:.2f}sec\tSize: {1}\tSeq Read".format(t.time() - start_time, sys.getsizeof(inp) + sys.getsizeof(out))

        start_time = t.time()
        #print np.sum(inp) + np.sum(out)
        print "Time: {1:.2f}sec\tSum: {0}\tSeq Read".format(np.sum(inp) + np.sum(out), t.time() - start_time)

        inp = []
        out = []
        start_time = t.time()
        for i in index:
            inp.append(f.root.raw.input[i] )
            out.append(f.root.raw.output[i] )

        print "Time: {0:.2f}sec\tSize: {1}\tRandom Read".format(t.time() - start_time, sys.getsizeof(inp) + sys.getsizeof(out))

        start_time = t.time()
        #print np.sum(inp) + np.sum(out)
        print "Time: {1:.2f}sec\tSum: {0}\tRandom Read".format(np.sum(inp) + np.sum(out), t.time() - start_time)


    # filename = "F_one_group_multiple_element_mnist.h5"
    # print filename
    # with tables.open_file(filename, mode='r') as f:
    #     start_time = t.time()
    #     for i in index:
    #         inp = f.get_node(f.root.raw.input, "m_{0}_0".format(i))[0]
    #         out = f.get_node(f.root.raw.output, "t_{0}_0".format(i))[0]
    #     print "Time: {0:.2f}sec Random Read".format(t.time() - start_time)

    #     start_time = t.time()
    #     inp = f.list_nodes(f.root.raw.input)
    #     out = f.list_nodes(f.root.raw.output)
    #     print "Time: {0:.2f}sec Seq Read".format((t.time() - start_time))

    #     start_time = t.time()
    #     for i in f.iter_nodes(f.root.raw.input):
    #         inp = i
    #     for i in f.iter_nodes(f.root.raw.output):
    #         out = i
    #     print "Time: {0:.2f}sec SeqIter Read".format((t.time() - start_time))
