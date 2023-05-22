import json
import argparse

parser = argparse.ArgumentParser(
                    prog='form parts from dataset')
parser.add_argument('k_part')  
args = parser.parse_args()

k_part = int(args.k_part)

writen = 0

total = 0
with open("../datasets/batch_instance.csv", "w") as wfile:
    with open("../datasets/batch_instance_.csv", "r") as file:
        line = file.readline()
        
        while len(line) != 0:
            job = line.split(',')[2]
            total += 1
            if int(job[2:]) // 100000 != k_part:
                line = file.readline()
                continue
            
            wfile.write(line)
            writen += 1
            line = file.readline()

print(f'total: {total}\nwriten: {writen}')
