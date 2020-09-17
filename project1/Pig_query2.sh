A = load ‘customer.txt’ using PigStorage(“,”) as (id:int, name: chararray, age:int, gender:chararray, countrycode:int, salary:float);
B = load ‘transaction.txt’ using PigStorage(“,”) as (transID: int, custID: int, transTotal: float, transNumItems: int, transDesc: chararray);
alpha = foreach A generate id, name, salary;
beta = foreach B generate custID, transTotal, transNumItems;
C = group beta by custID;
D = foreach C generate group as CustomerID, COUNT(beta.transTotal) as NumOfTransactions, SUM(beta.transTotal) as TotalSum, MIN(beta.transNumItems) as MinItems;
E = join D by CustomerID, alpha by id using ‘replicated’;
F = foreach E generate CustomerID, name, salary, NumOfTransactions, TotalSum, MinItems; 
store F into ‘/user/mqp/output/pig2’;