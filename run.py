import sys

if __name__ == "__main__":
    inputs = sys.argv[1:]
    if len(inputs) % 2 != 0:
        print("\nIll defined input. Please define the desired number of samples following the csv.\n\nE.g.: python3 run.py sample1.py 2 sample2.py 4\n")
        sys.exit()
    else:
    
    for csv in inputs()