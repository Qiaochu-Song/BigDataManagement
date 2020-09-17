A = load ‘customer.txt’ using PigStorage(“,”) as (id:int, name: chararray, age:int, gender:chararray, countrycode:int, salary:float);
B = load ‘transaction.txt’ using PigStorage(“,”) as (transID: int, custID: int, transTotal: float, transNumItems: int, transDesc: chararray);
C = group B by custID;
D = foreach C generate group as custID, COUNT(B.transTotal) as transCount;
E = order D by transCount; 
minCount = limit E 1;
minCust = join D by transCount, minCount by transCount;
F = join A by id, minCust by D::custID;
G = foreach F generate A::name, D::transCount;
store E into ‘/user/mqp/output/pig1’;


