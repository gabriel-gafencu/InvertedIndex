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

```mpiexec -n <number-of-processes> python3 main.py <input-dir> <output-dir>``` where: 
<li> <code>number-of-processes</code> is the number of running processes (must be greater than 4)</li>
<li> <code>input-dir</code> is the directory that contains the input files</li>
<li> <code>output-dir</code> is the directory created at run time which contains the output files</li>

The testcases can be executed using the below command:

```python3 -m unittest test_cases.py``` where:
<li>test_cases.py is located in test-cases</li>

