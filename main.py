from mpi4py import MPI
import sys
import re
import json
import os

BEGIN = 0
END = 1

N_RESERVED_RANKS = 4

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

master = 0
mappers = [] #[1, 2, 3, 4, 5]
reducers = [1, 2, 3]  #[6, 7, 8]

if len(sys.argv) != 2:
    if size <= N_RESERVED_RANKS:
        raise Exception("Number of processes must be greater than 4.")
    else:
        mappers = [x for x in range(N_RESERVED_RANKS, size)]
else:
    raise Exception("Program taken {} arguments but {} provided.\nExample: python <input-dir> <output-dir>\n".format(2, len(sys.argv)))

INPUT_DIR = sys.argv[1]
BASE_DIR = sys.argv[2]
MAPPERS_DIR = BASE_DIR+"/map"
REDUCERS_DIR = BASE_DIR+"/reduce"

# if rank == 0:
#     pass
# else:
#     sys.exit(0)



'''
Deoarece algoritmul de calcul a Inverted Index este bazat pe algoritmul map-reduce, care se foloseste de paradigma 
programarii paralele, se vor indica complexitatile si demonstrarea corectitudinii din punctul de vedere al unui singur nod de procesare.
Acest lucru este posibil, deoarece procesele realizeaza in paralel task-urile, rezultatul-ul final fiind concatenarea rezultatelor partiale.
Deci fiecare nod de procesare va avea un anumit set de date de intrare de prelucrat.

Ideea acestui algoritm este de obtine o structura de date(in cazul nostru o structura de fisere) care sa indice in
ce structuri de intrare(o serie de fisiere ce contin text) se afla un anumit cuvant si de cate ori apare.



Se demonstreaza corectitudinea pentru secventa de reducere a fiecarui proces.
    Aici se iau fisierele generate de procesele de tip mapper, care contin cuvintele in ordine alfabetica si fisierele din care provin,
    si se vor genera fisierele finale care contin sub forma de dictionare cuvintele prezente in texte, locatiile in care se gasesc si numarul
    de aparitii.

    Un proces care realizeaza procesul de reduce se va ocupa doar de un interval de cuvinte(de ex. cuintele care incep cu literele a-f).
    Datele din fisirele finale vor fi de forma:

    "<cuvant_care_incepe_cu_a>": {
            "<nume_fisier_1.txt>" : <numarul_de_aparitii_ale_cuvantului_in_fisierul_1>,
            "<nume_fisier_2.txt>" : <numarul_de_aparitii_ale_cuvantului_in_fisierul_2>,
            "<nume_fisier_3.txt>" : <numarul_de_aparitii_ale_cuvantului_in_fisierul_3>,
            ...
        },
    "<cuvant_care_incepe_cu_b>": {
            "<nume_fisier_1.txt>" : <numarul_de_aparitii_ale_cuvantului_in_fisierul_1>,
            "<nume_fisier_2.txt>" : <numarul_de_aparitii_ale_cuvantului_in_fisierul_2>,
            "<nume_fisier_3.txt>" : <numarul_de_aparitii_ale_cuvantului_in_fisierul_3>,
            ...
        },
        ...

    terms - este un dictionar de dictionare

    Pentru algortimul de reduce, pentru un singur nod, avem:
    Preconditia P: Avem o lista de fisiere de prelucrat(n>0, unde n este numarul de fisiere)
    Postconditia Q: Sa avem lista de dictionare care vor avea cuvintele mapate catre fisierele din care provin si numarul de aparitii.

    Alte notatii de ajutor:
    l - numarul de linii(cuvinte - fisier sursa) dintr-un fisier.
    n - numarul de fisiere de prelucrat

    Pentru while-ul care se ocupa de citirea cuvintelor dintr-un fisier.

        Invariantul I este: Pentru fiecare cuvant citit, dictionarul terms contine dictionarul de fisiere sursa mapat la numarul de aparitii pentru acel cuvant.
            Demonstratie prin inductie: 
                Pasul 1: Inainte de rularea buclei, inca nu am citit nicio linie, deci dictionarul este gol(Adevarat)
                Pasul k: La pasul k, citim cuvantul de pe linia k. Presupunem ca dictionarul a fost actualizat corect pana acum.
                Pasul k+1:
                    Citim din fisier cuvantul si fisierul din care provine.
                    Daca cuvantul se gaseste deja in dicionarul terms, atunci:
                        Daca in dictionar se gaseste dictionarul pentru fisierul din care provine cuvantul(terms[word] este tot un dictionar), 
                        atunci actualizez numarul de aparitii pentru cuvantul respectiv in acel fisier, deci terms[word][original_file] += 1, unde word este cuvantul curent iar 
                        original_file este fisierul din care provine.

                        Daca in dicionar, nu se gaseste intrarea pentru fisier-ul respectic, atunci se pune in dictionar prima aparitie a cuvantului, deci terms[word][original_file] = 1, 
                        unde word este cuvantul curent iar original_file este fisierul din care provine.

                    Daca cuvantul nu se gaseste in dictionar, atunci se creaza intrarea pentru acel cuvant si pentru fisierul de unde provine si se 
                    pune in dictionar prima aparitie a cuvantului, deci terms[word][original_file] = 1, unde word este cuvantul curent iar original_file este fisierul din care provine. 

                Deci, in concluzie, pentru fiecare cuvant citit, dictionarul terms contine dictionarul de fisiere sursa mapat la numarul de aparitii pentru acel cuvant.

        Functia de terminare
            Aici avem un caz special, deoarece din bucla se iese atunci cand instructiunea line = next(f).split() arunca o exceptie de tipul StopIteration, care indica faptul 
            ca s-au terminat cuvintele din fisier.
            Deci, putem spune ca functia de terminare este de tipul: f(j) = l - j, unde j este indexul liniei curente care se citeste, iar l este numarul total de linii.
            Prin urmare, while True: ar putea deveni while j < l: #j += 1 in interiorul while-ului
            In cazul acesta, avem: 
            conditia c: j < l

            a)La inceputul prelucrarii, I este adevarata (demonstrat prin inductie)
            b)I este adevarata inainte de prelucrarea din interiorul while-ului si j < l(adevarat), atunci I este adevarata si dupa prelucrare
            c)La finalul while-ului, c define falsa, si dupa prelucrare I si non c -> I (adevarat)
            d)După efectuarea prelucrării din interiorul while-ulu, valoarea lui f descreste(adevarat), caci 
            f(0) = l - 0 = 10
            f(1) = l - 1 = 9
            ...
            f(10) = l - l = 0(in cod aceasta este si situatia care va genera exceptia StopIteration), 

            e)Algoritmul se termină după un număr finit de pași(adevarat), caci functia de terminare descreste spre 0.


    Pentru for-ul responsabil de fisiere
        Invariantul I: In fisierul de output am scris lista de dictionare pentru fisierul curent de cuvinte care incep cu o anumita litera.
            Demonstratie prin inductie
                Pasul 1: La inceput, nu am niciun dictionar in lista, deoarece inca nu am citit niciun fisier curent de cuvinte.
                Pasul k: La pasul k deschid al k-lea fisier sa citesc cuvintele. Stiu ca toate cuvintele din fisier incep cu aceeasi litera, dar provin din fisiere initiale diferite.
                Pasul k+1: Stiu ca pana in acest moment am scris in fisierul de output lista ce contine dictionarele pana la litera curenta(acest lucru a fost demonstrat deja, deoarece 
                s-a demonstrat corectitudinea pentru while-ul care se ocupa de citirea cuvintelor din fisier)

                Deci, in fisierul de output am scrisa lista de dictionare pentru toate fisierele de intrare.

        Functia de terminare
            In cod avem instructiunea for scrisa in modul urmator: for file in my_file:, pentru a itera prin lista de fisiere de intrare.
            Cum n>0 este numarul de fisiere de intrare, atunci putem scrie functia de terminare ca fiind:
            f(i) = n - i, unde n este numarul de fisiere de intrare, iar i este indexul fisierului curent(cu i de la o la n-1)

            In cazul acesta, avem: 
            conditia c: i < n
            a)La inceputul prelucrarii, I este adevarata (demonstrat prin inductie)
            b)I este adevarata inainte de prelucrarea din interiorul while-ului si i < n(adevarat), atunci I este adevarata si dupa prelucrare
            c)La finalul while-ului, c define falsa, intr-adevar, c: i < n, caci i va depasi n, deci i si non c -> I
            d)După efectuarea prelucrării din interiorul while-ulu, valoarea lui f descreste(adevarat), caci 
            f(0) = n - 0 = n
            f(1) = n - 1
            ...
            f(n) = n - n = 0
            e)Algoritmul se termină după un număr finit de pași(adevarat), caci functia de terminare descreste spre 0.



'''


if rank == master:
    # Master delegates tasks according to rank and orchestrates the map-reduce process

    # Printing status for curent execution
    print("\n\nProgram started")
    print("Master rank: {}".format(master))
    print("Mappers: {}".format(mappers))
    print("Reducers: {}".format(reducers))
    print("Input directory: {}".format(INPUT_DIR))
    print("Output directory: {}\n".format(BASE_DIR))


    # Creating directories for storing the results
    
    if os.path.exists(BASE_DIR) == False:
        os.mkdir(BASE_DIR)
    
    if os.path.exists(MAPPERS_DIR) == False:
        os.mkdir(MAPPERS_DIR)

    if os.path.exists(REDUCERS_DIR) == False:
        os.mkdir(REDUCERS_DIR)

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

    N_MAPPERS = size - N_RESERVED_RANKS
    MAPPER_INDEX = rank - N_RESERVED_RANKS

    all_files = [file for file in os.listdir(INPUT_DIR)]
    files_per_process = int(len(all_files) / N_MAPPERS)

    my_files = [all_files[file_index] for file_index in range(MAPPER_INDEX * files_per_process, (MAPPER_INDEX + 1) * files_per_process)]
    
    for file_index in range(N_MAPPERS * files_per_process, len(all_files)):
        if file_index % files_per_process == MAPPER_INDEX:
            my_files.append(all_files[file_index]) 

    print("Process rank {} start to process files: {}".format(rank, my_files))

    for file_name in my_files:
        try:
            with open('{}/{}'.format(INPUT_DIR, file_name), 'r') as f:
                while True:
                    try:
                        line = next(f)
                        words = line.split()
                        for word in words:
                            # make all words lower case as searching for terms is case insensitive
                            word = re.sub('[^a-zA-Z]+', '', word.lower())
                            if len(word) > 0:
                                file = open('{}/{}.txt'.format(MAPPERS_DIR, word[0]), 'a')
                                file.write('{} {}\n'.format(word, file_name))
                                file.close()
                    except StopIteration:
                        break
                    except UnicodeError:
                        # some of the files might raise decoding errors
                        # in this case simply ignore the line and move on to the next one
                        pass
        except FileNotFoundError:
            # this happends when some words miss becouse of lack of text
            # we simply continue
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
    my_files = ['{}.txt'.format(chr(x)) for x in range(ord(limiters[reducer_rank]['start']), ord(limiters[reducer_rank]['stop']) + 1)]
    end_file = '{}/{}-{}.txt'.format(REDUCERS_DIR,limiters[reducer_rank]['start'], limiters[reducer_rank]['stop'])

    #de aici demonstrez corectitudinea pentru reducer
    for file in my_files:
        terms = {}
        try:
            with open('{}/{}'.format(MAPPERS_DIR, file), 'r') as f:
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
        except FileNotFoundError:
            # this happends when some words miss becouse of lack of text
            # we simply continue
            pass

        # sort terms dict by terms
        terms = dict(sorted(terms.items()))
        # then sort every term by file docID (file name)
        for word in terms:
            terms[word] = dict(sorted(terms[word].items()))

        pretty_terms = json.dumps(terms, indent=4)
        write_file = open(end_file, 'a')
        write_file.write('{}\n~\n'.format(pretty_terms))
        write_file.close()

    comm.send('end', dest=master, tag=END)
