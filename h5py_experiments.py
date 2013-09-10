from __future__ import division
import h5py
import time as t
import numpy as np
import os

rng = np.random.RandomState(1234)
nb_examples = 70000
complibs = ['blosc', 'lzo', 'zlib', 'bzip2']

def zero_group_one_element(nb_examples, root):
        ##### One Matrix for all input
        root.create_dataset('input', data=rng.randint(0, 2, (nb_examples, 28, 28)).astype(np.bool))
        root.create_dataset('output', data=rng.randint(0, 10, nb_examples).astype(np.uint8))


def one_group_multiple_element(nb_examples, root):
        ##### One group with multiple elements
        input_data = root.create_group('input')
        for i in range(nb_examples):
            input_data.create_dataset('m_{0}_{1}'.format(i, 0), data=rng.randint(0, 2, (28,28)).astype(np.bool))

        output_data = root.create_group('output')
        for i in range(nb_examples):
            output_data.create_dataset('t_{0}_{1}'.format(i, 0), data=rng.randint(0, 10, 1))


def mutiple_group_one_element(nb_examples, root):
        ##### One group per example
        input_data = root.create_group('input')
        for i in range(nb_examples):
            example_i = input_data.create_group('example{0}'.format(i))
            example_i.create_dataset('m_{0}_{1}'.format(i, 0), data=rng.randint(0, 2, (28,28)).astype(np.bool))

        output_data = root.create_group('output')
        for i in range(nb_examples):
            target_i = output_data.create_group('target{0}'.format(i))
            target_i.create_dataset('t_{0}_{1}'.format(i, 0), data=rng.randint(0, 10, 1))

def write_file(write_method):
    # for complib in complibs:
    #     my_filters = tables.filters.Filters(complevel=9, complib=complib)
    #     filename = write_method.__name__+'_mnist_'+ complib +'_c9.h5'
    #     with tables.open_file(filename, mode='w', filters=my_filters, max_group_width=nb_examples) as f:
    #         raw = f.create_group('/', 'raw')
    #         start_time = t.time()
    #         write_method(complib, nb_examples, f, raw, f.create_carray)
    #         print "Name:{0}\tTime:{1:.4f} sec\tSize:{2:.4f} Mb".format(filename, t.time() - start_time, os.path.getsize(filename)/1024/1024)

    filename = write_method.__name__+'_mnist_c0.h5'
    with h5py.File(filename, 'w') as f:
        raw = f.create_group('raw')
        start_time = t.time()
        write_method(nb_examples, raw)
        print "Name:{0}\tTime:{1:.4f} sec\tSize:{2:.4f} Mb".format(filename, t.time() - start_time, os.path.getsize(filename)/1024/1024)

##### MAIN #####
if __name__ == "__main__":
    write_file(one_group_multiple_element)
    write_file(mutiple_group_one_element)
    write_file(zero_group_one_element)
