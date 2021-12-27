### Description

A program for calculating the inverted index using map-reduce and OpenMPI.

It takes input ````.txt```` files from the ```test-files``` directory, parses them (as seen in the ```intermediary/map``` sub-directory)
and then outputs the final result in the ```intermediary/reduce``` sub-directory.

### Setup

Run below command first to install requirements.

```python -m pip install -r requirements.txt```

Also, you need to have *mpi* and *mpisdk* installed and added in the ```PATH``` variable in order to run the program.

### Run

The script can be executed using the below command:

```mpiexec -n 9 python main.py```