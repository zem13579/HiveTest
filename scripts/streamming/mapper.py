import sys
for line in sys.stdin:
    words = line.strip().split()
    for word in words:
        print("%s\t1",word.lower())
