#!/usr/bin/python

import numpy as np

# label need to be 0 to num_class -1
data = np.loadtxt('./dermatology.data', delimiter=',',
                  converters={33: lambda x:int(x == '?'), 34: lambda x:int(x) - 1})
sz = data.shape

train = data[:int(sz[0] * 0.7), :]
test = data[int(sz[0] * 0.7):, :]

train_X = train[:, :33]
train_Y = train[:, 34]

test_X = test[:, :33]
test_Y = test[:, 34]

def process(X, Y):
    s = ""
    for i in range(len(X)):
        s += str(int(Y[i]))
        for j in range(len(X[i])):
            s += (" %d:%d" % (j, int(X[i][j])))
        s += "\n"
    return s

with open("./dermatology.data.train", 'w') as fp:
    fp.write(process(train_X, train_Y))

with open("./dermatology.data.test", 'w') as fp:
    fp.write(process(test_X, test_Y))