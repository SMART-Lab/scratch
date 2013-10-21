from __future__ import division
import h5py
import time as t
import numpy as np
import os

rng = np.random.RandomState(1234)
nb_examples = 70000
complibs = ['gzip', 'lzf']

def zero_group_one_element(nb_examples, root, compression):
        ##### One Matrix for all input
        root.create_dataset('input',
                            data=rng.randint(0, 2, (nb_examples, 28, 28)).astype(np.bool),
                            compression=compression,
                            chunks=(1024, 28, 28))
        root.create_dataset('output', data=rng.randint(0, 10, nb_examples).astype(np.uint8), compression=compression)


def one_group_multiple_element(nb_examples, root, compression):
        ##### One group with multiple elements
        input_data = root.create_group('input')
        for i in range(nb_examples):
            input_data.create_dataset('m_{0}_{1}'.format(i, 0), data=rng.randint(0, 2, (28,28)).astype(np.bool), compression=compression)

        output_data = root.create_group('output')
        for i in range(nb_examples):
            output_data.create_dataset('t_{0}_{1}'.format(i, 0), data=rng.randint(0, 10, 1), compression=compression)


def mutiple_group_one_element(nb_examples, root, compression):
        ##### One group per example
        input_data = root.create_group('input')
        for i in range(nb_examples):
            example_i = input_data.create_group('example{0}'.format(i))
            example_i.create_dataset('m_{0}_{1}'.format(i, 0), data=rng.randint(0, 2, (28,28)).astype(np.bool), compression=compression)

        output_data = root.create_group('output')
        for i in range(nb_examples):
            target_i = output_data.create_group('target{0}'.format(i))
            target_i.create_dataset('t_{0}_{1}'.format(i, 0), data=rng.randint(0, 10, 1), compression=compression)

def write_file(write_method):
    for complib in complibs:
        filename = write_method.__name__+'_mnist_'+ complib +'_c9.h5'
        with h5py.File(filename, 'w') as f:
            raw = f.create_group('raw')
            start_time = t.time()
            write_method(nb_examples, raw, complib)
            print "Name:{0}\tTime:{1:.4f} sec\tSize:{2:.4f} Mb".format(filename, t.time() - start_time, os.path.getsize(filename)/1024/1024)

    filename = write_method.__name__+'_mnist_c0.h5'
    with h5py.File(filename, 'w') as f:
        raw = f.create_group('raw')
        start_time = t.time()
        write_method(nb_examples, raw, None)
        print "Name:{0}\tTime:{1:.4f} sec\tSize:{2:.4f} Mb".format(filename, t.time() - start_time, os.path.getsize(filename)/1024/1024)

##### MAIN #####
if __name__ == "__main__":
    write_file(zero_group_one_element)
    write_file(one_group_multiple_element)
    write_file(mutiple_group_one_element)
