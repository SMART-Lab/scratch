import tables
import numpy as np

rng = np.random.RandomState(1234)

nb_examples = 70000

#fileh = tables.open_file('seq1.h5', mode='r')
my_filters = tables.filters.Filters(complevel=0)
with tables.open_file('seq1.h5', mode='w', filters=my_filters) as fileh:
    raw = fileh.create_group('/', 'raw')

    input_data = fileh.create_carray(raw, 'input', obj=rng.randint(0, 255, (nb_examples, 28, 28)).astype(np.uint8))
    output_data = fileh.create_carray(raw, 'output', obj=rng.randint(0, 9, nb_examples).astype(np.uint8))


    # input_data = fileh.create_group(raw, 'input')
    # for i in range(nb_examples):
    #     example_i = fileh.create_group(input_data, 'example{0}'.format(i))
    #     #for j in range(rng.randint(1, 28*28)):
    #         #m_i_j = fileh.create_carray(example_i, 'm_{0}_{1}'.format(i, j), obj=np.eye(rng.randint(0, 255)) * (i * nb_examples + j))
    #     m_i_j = fileh.create_carray(example_i, 'm_{0}_{1}'.format(i, 0), obj=rng.randint(0, 1, (28,28)).astype(np.bool))

    # output_data = fileh.create_group(raw, 'output')
    # for i in range(nb_examples):
    #     target_i = fileh.create_group(output_data, 'target{0}'.format(i))
    #     #for j in range(rng.randint(1, 1)):
    #     #    t_i_j = fileh.create_carray(target_i, 't_{0}_{1}'.format(i, j), obj=np.diag(rng.randint(0, 9, rng.randint(1, 1))))
    #     t_i_j = fileh.create_carray(target_i, 't_{0}_{1}'.format(i, 0), obj=rng.randint(0, 9, (1,1)).astype(np.uint8))


    # print ""
    # print "Display examples"
    # print ""
    # #Display examples
    # for example_i in fileh.iter_nodes(where=fileh.root.raw.input):
    #     for m_i_j in fileh.iter_nodes(where=example_i):
    #         print m_i_j.read()

    #     print "-------------"


    # #Display targets
    # print ""
    # print "Display targets"
    # print ""
    # for target_i in fileh.iter_nodes(where=fileh.root.raw.output):
    #     for t_i_j in fileh.iter_nodes(where=target_i):
    #         print t_i_j.read()

    #     print "-------------"


#fileh = tables.open_file('seq1.h5', mode='r')
