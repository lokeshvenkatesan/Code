import csv

%precision 2

with open(mpg.csv) as csvFile:
  mpg = csv.DictReader(csvFile)
  
mpg[:3]  