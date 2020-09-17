A = load ‘customer.txt’ using PigStorage(“,”) as (id:int, name: chararray, age:int, gender:chararray, countrycode:int, salary:float);
B = load ‘transaction.txt’ using PigStorage(“,”) as (transID: int, custID: int, transTotal: float, transNumItems: int, transDesc: chararray);
C = group A by countrycode;
D = foreach C generate group as countrycode, COUNT(A.id) as numCustomers;
E = filter D by numCustomers>5000 or numCustomers<2000;
store E into ‘/user/mqp/output/query3’;