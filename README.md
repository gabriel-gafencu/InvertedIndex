A program for calculating the inverted index using map-reduce and OpenMPI.

It takes input ````.txt```` files from the ```test-files``` directory, parses them (as seen in the ```intermediary/map``` sub-directory)
and then outputs the final result in the ```intermediary/reduce``` sub-directory.

The script can be run using the below command:

```mpiexec -n 9 python main.py```