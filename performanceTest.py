import subprocess
import time
import sys

def standard(count):
    print "Testing %u increments on a single variable with redis-cli ..." % count,
    sys.stdout.flush()
    stdin = "SET hi 1\n"
    for i in range(count):
        stdin += "INCR hi\n"
    stdin += "GET hi\n"
    tic = time.time()
    proc = subprocess.Popen(["redis-cli"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate(stdin)
    toc = time.time()
    print "took %fsec" % (toc - tic)

def redish(count):
    print "Testing %u increments on a single variable with redish ..." % count,
    sys.stdout.flush()
    stdin = '{"command": "CONNECT"}\n'
    stdin += '{"id": 1, "command": "SET", "args":["hi", 1]}\n'
    for i in range(count):
        stdin += '{"id": 1, "command": "INCR", "args":["hi"]}\n'
    stdin += '{"id": 1, "command": "GET", "args":["hi"]}\n'
    tic = time.time()
    proc = subprocess.Popen(["python", "redish.py", "100000"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate(stdin)
    toc = time.time()
    print "took %fsec" % (toc - tic)

def standardBig(count):
    print "Writing to %u keys with SET using redis-cli ..." % count,
    stdin = ""
    for i in range(count):
        stdin += "SET hi%u\n" % count
    tic = time.time()
    proc = subprocess.Popen(["redis-cli"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate(stdin)
    toc = time.time()
    print "took %fsec" % (toc - tic)

def redishBig(count):
    print "Writing to %u keys with SET using redish ..." % count,
    stdin = '{"command": "CONNECT"}\n'
    for i in range(count):
        stdin += '{"id": 1, "command": "SET", "args":["hi%u", %u]}\n' % (i, i)
    stdin += '{"id": 1, "command": "GET", "args":["hi%u"]}\n' % i
    tic = time.time()
    proc = subprocess.Popen(["python", "redish.py", "%u" % count], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate(stdin)
    toc = time.time()
    print "took %fsec" % (toc - tic)

count = 100000
standard(count)
redish(count)
standardBig(count)
redishBig(count)

