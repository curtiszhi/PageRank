'''
Created on Jun 15, 2016

@author: curtiszhi
'''
import Constants
from random import randint

with open("Simulation.txt", "w+") as f:
    for _ in xrange(Constants.N * 10):
        f.write(str(randint(0, Constants.N - 1)) + " " + str(randint(0, Constants.N - 1)) + "\n")