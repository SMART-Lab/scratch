from __future__ import division
#from ipdb import set_trace as dbg

import h5py
import time as t
import numpy as np
import os

from functools import partial

rng = np.random.RandomState(1234)
nb_examples = 70000
complibs = ['gzip', 'lzf']
chunk_sizes = [1, 50, 1024]

def zero_group_one_element(nb_examples, root, compression, chunk_size=None):
    ##### One Matrix for all input
    if chunk_size is None:
        root.create_dataset('input',
                            data=rng.randint(0, 2, (nb_examples, 28, 28)).astype(np.bool),
                            compression=compression)
        
        root.create_dataset('output',
                            data=rng.randint(0, 10, nb_examples).astype(np.uint8),
                            compression=compression)
    else:
        root.create_dataset('input',
                            data=rng.randint(0, 2, (nb_examples, 28, 28)).astype(np.bool),
                            compression=compression,
                            chunks=(chunk_size, 28, 28))
        
        root.create_dataset('output',
                            data=rng.randint(0, 10, nb_examples).astype(np.uint8),
                            compression=compression,
                            chunks=(chunk_size, ))


def one_group_multiple_element(nb_examples, root, compression):
    ##### One group with multiple elements
    input_data = root.create_group('input')
    for i in range(nb_examples):
        input_data.create_dataset('m_{0}_{1}'.format(i, 0),
                                  data=rng.randint(0, 2, (28,28)).astype(np.bool),
                                  compression=compression)

    output_data = root.create_group('output')
    for i in range(nb_examples):
        output_data.create_dataset('t_{0}_{1}'.format(i, 0),
                                   data=rng.randint(0, 10, 1).astype(np.uint8),
                                   compression=compression)


def mutiple_group_one_element(nb_examples, root, compression):
    ##### One group per example
    input_data = root.create_group('input')
    for i in range(nb_examples):
        example_i = input_data.create_group('example{0}'.format(i))
        example_i.create_dataset('m_{0}_{1}'.format(i, 0),
                                 data=rng.randint(0, 2, (28,28)).astype(np.bool),
                                 compression=compression)

    output_data = root.create_group('output')
    for i in range(nb_examples):
        target_i = output_data.create_group('target{0}'.format(i))
        target_i.create_dataset('t_{0}_{1}'.format(i, 0),
                                data=rng.randint(0, 10, 1).astype(np.uint8),
                                compression=compression)

def _write_file(write_method, f, filename, complib):
    raw = f.create_group('raw')
    start_time = t.time()
    write_method(nb_examples, raw, complib)
    print "\tCompLib: {0: <3}\tTime: {1:.2f}sec\tSize: {2:.2f}Mb".format(complib, t.time() - start_time, os.path.getsize(filename) / 1024 / 1024)


def write_file(write_method, dataset_name):
    print write_method.__name__
    for complib in complibs:
        filename = write_method.__name__ + '_' + dataset_name + '_' + complib + '_c9.h5'
        with h5py.File(filename, mode='w') as f:
            _write_file(write_method, f, filename, complib)
    #     os.remove(filename)

    filename = write_method.__name__ + '_' + dataset_name + '.h5'
    with h5py.File(filename, mode='w') as f:
        _write_file(write_method, f, filename, None)
    #    os.remove(filename)


##### MAIN #####
if __name__ == "__main__":
    dataset_name = "h5py_mnist"
    print "Dataset like " + dataset_name
    for chunk_size in chunk_sizes:
        fct = partial(zero_group_one_element, chunk_size=chunk_size)
        fct.__name__ = zero_group_one_element.__name__ + "_" + str(chunk_size)
        write_file(fct, dataset_name)

    write_file(zero_group_one_element, dataset_name)
    write_file(one_group_multiple_element, dataset_name)
    write_file(mutiple_group_one_element, dataset_name)
