from __future__ import division
import tables
import time as t
import numpy as np
import os

rng = np.random.RandomState(1234)
nb_examples = 70000
complibs = ['blosc', 'lzo', 'zlib', 'bzip2']


def zero_group_one_element(nb_examples, f, raw, create):
        # One Matrix for all input
        create(raw, 'input', obj=rng.randint(0, 2, (nb_examples, 28, 28)).astype(np.bool))
        create(raw, 'output', obj=rng.randint(0, 10, nb_examples).astype(np.uint8))


def one_group_multiple_element(nb_examples, f, raw, create):
        # One group with multiple elements
        input_data = f.create_group(raw, 'input')
        for i in range(nb_examples):
            create(input_data, 'm_{0}_{1}'.format(i, 0), obj=rng.randint(0, 2, (28, 28)).astype(np.bool))

        output_data = f.create_group(raw, 'output')
        for i in range(nb_examples):
            create(output_data, 't_{0}_{1}'.format(i, 0), obj=rng.randint(0, 10, 1))


def mutiple_group_one_element(nb_examples, f, raw, create):
        # One group per example
        input_data = f.create_group(raw, 'input')
        for i in range(nb_examples):
            example_i = f.create_group(input_data, 'example{0}'.format(i))
            create(example_i, 'm_{0}_{1}'.format(i, 0), obj=rng.randint(0, 2, (28, 28)).astype(np.bool))

        output_data = f.create_group(raw, 'output')
        for i in range(nb_examples):
            target_i = f.create_group(output_data, 'target{0}'.format(i))
            create(target_i, 't_{0}_{1}'.format(i, 0), obj=rng.randint(0, 10, 1))


def _write_file(write_method, f, filename, create, complib):
    raw = f.create_group('/', 'raw')
    start_time = t.time()
    write_method(nb_examples, f, raw, create)
    print "\tCompLib: {0: <3}\tTime: {1:.2f}sec\tSize: {2:.2f}Mb".format(complib, t.time() - start_time, os.path.getsize(filename) / 1024 / 1024)

def write_file(write_method, dataset_name):
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
    dataset_name = "mnist"
    print "Dataset like " + dataset_name
    write_file(one_group_multiple_element, dataset_name)
    write_file(mutiple_group_one_element, dataset_name)
    write_file(zero_group_one_element, dataset_name)
