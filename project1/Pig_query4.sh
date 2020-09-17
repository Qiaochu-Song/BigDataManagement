A = load ‘customer.txt’ using PigStorage(“,”) as (id:int, name: chararray, age:int, gender:chararray, countrycode:int, salary:float);
B = load ‘transaction.txt’ using PigStorage(“,”) as (transID: int, custID: int, transTotal: float, transNumItems: int, transDesc: chararray);
C = foreach A generate (age <20 ? ‘[10,20)’:(age<30 ? ‘[20,30)’ : (age<40 ? ‘[30,40)’:(age<50 ? ‘[40,50)’:(age<60? ‘[50,60)’: ‘[60,70]’))))) as ageRange, id, gender;
D = join C by id, B by custID;
E = group D by (C::ageRange, C::gender). 
F = foreach E generate group, MIN(D.B::transTotal) as MinTransTotal, MAX(D.B::transTotal) as MaxTransTotal, AVG(D.B::transTotal) as AVGTransTotal;
store F into ‘/user/mqp/output/output4’;