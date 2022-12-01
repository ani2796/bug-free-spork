# Advanced DB Final Project

**Author**: Anirudh Sriram
**Net ID**: as9913
**Submission Date**: Dec 1, 2022

Implementation of a distributed DB with multiversion read consistency, deadlock detection, replication and failure recovery.  
  
Some testing notes are below. Please find more detailed notes about the code in `/src/simulator.py`.  

## Testing Notes
The test transaction sets, as derived from the course website, are present in `/trans-sets`. All these tests can be run from the root directory using:  
  
`python3 src/simulator.py`  
  
As also stated in `/src/simulator.py`, individual test files can be run using the following command(s):  
  
`python3 src/simulator.py [ -f | --file ] <f_name>`   
  
The output for `f_name` will be stored in `f_name-out` in the same relative path as `f_name`, which is a path, and must be relative to the root dir.