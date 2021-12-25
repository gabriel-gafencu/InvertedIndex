from mpi4py import MPI
import sys
import re
import json

BEGIN = 0
END = 1

master = 0
mappers = [1, 2, 3, 4, 5]
reducers = [6, 7, 8]

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank == master:
    # Master delegates tasks according to rank and orchestrates the map-reduce process

    # tell mapper processes to begin
    for mapper in mappers:
        comm.send('begin', dest=mapper, tag=BEGIN)
    # wait for mappers to finish
    for mapper in mappers:
        comm.recv(source=mapper, tag=END)

    print('Mappers finished.')
    print('Beginning reduce phase.')
    sys.stdout.flush()

    # tell reducer processes to begin
    for reducer in reducers:
        comm.send('begin', dest=reducer, tag=BEGIN)
    # wait for reducers to finish
    for reducer in reducers:
        comm.recv(source=reducer, tag=END)

    print('Reducers finished.')
    print('Map-reduce process finished.')
    sys.stdout.flush()

elif rank in mappers:
    # wait for master to signal start
    comm.recv(source=master, tag=BEGIN)
    print('{} beginning mapping process.'.format(rank))
    sys.stdout.flush()

    # each mapper is responsible for 5 files:
    # process 1 for 1-5, process 2 for 6-10, ect.
    my_files = [x for x in range((rank - 1) * 5 + 1, rank * 5 + 1)]
    for file_number in my_files:
        file_name = '{}.txt'.format(file_number)
        with open('test-files/{}'.format(file_name), 'r') as f:
            while True:
                try:
                    line = next(f)
                    words = line.split()
                    for word in words:
                        # make all words lower case as searching for terms is case insensitive
                        word = re.sub('[^a-zA-Z]+', '', word.lower())
                        if len(word) > 0:
                            file = open('intermediary/map/{}.txt'.format(word[0]), 'a')
                            file.write('{} {}\n'.format(word, file_name))
                            file.close()
                except StopIteration:
                    break
                except UnicodeError:
                    # some of the files might raise decoding errors
                    # in this case simply ignore the line and move on to the next one
                    pass

    comm.send('end', dest=master, tag=END)

elif rank in reducers:
    # wait for signal to start
    comm.recv(source=master, tag=BEGIN)
    print('{} beginning first reduce process.'.format(rank))
    sys.stdout.flush()

    # 3 reducers work on files from map phase as such:
    # reducer 1 is responsible for files a-f
    # reducer 2 is responsible for files g-p
    # reducer 3 is responsible for files q-z
    limiters = {
        '0': {
            'start': 'a',
            'stop': 'f'
        },
        '1': {
            'start': 'g',
            'stop': 'p'
        },
        '2': {
            'start': 'q',
            'stop': 'z'
        }
    }
    reducer_rank = str(rank - reducers[0])
    my_files = ['{}.txt'.format(chr(x)) for x in
                range(ord(limiters[reducer_rank]['start']), ord(limiters[reducer_rank]['stop']) + 1)]
    end_file = 'intermediary/reduce/{}-{}.txt'.format(limiters[reducer_rank]['start'], limiters[reducer_rank]['stop'])

    for file in my_files:
        terms = {}
        with open('intermediary/map/{}'.format(file), 'r') as f:
            while True:
                try:
                    line = next(f).split()
                    if len(line) == 2:
                        word = line[0]
                        original_file = line[1]

                        if word in terms:
                            if original_file in terms[word]:
                                terms[word][original_file] += 1
                            else:
                                terms[word][original_file] = 1
                        else:
                            terms[word] = {}
                            terms[word][original_file] = 1

                except StopIteration:
                    break
                except UnicodeError:
                    # some of the files might raise decoding errors
                    # in this case simply ignore the line and move on to the next one
                    pass

        # sort terms dict by terms
        terms = dict(sorted(terms.items()))
        # then sort every term by file docID (file name)
        for word in terms:
            terms[word] = dict(sorted(terms[word].items()))

        pretty_terms = json.dumps(terms, indent=4)
        write_file = open(end_file, 'a')
        write_file.write('{}\n'.format(pretty_terms))
        write_file.close()

    comm.send('end', dest=master, tag=END)
