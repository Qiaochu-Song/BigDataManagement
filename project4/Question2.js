// 2.1 Write an aggregation query that group by the award name, i.e., the “award” field inside the “awards” array and reports the count of each award.
print("Question 2.1 aggregation: ");
var temp = db.test.aggregate([{$unwind:"$awards"},{$group: {"_id":"$awards.award", "count":{$sum:1}}} ]);
while(temp.hasNext()){
	printjson(temp.next());
}

// 2.2 Write an aggregation query that groups by the birth year, i.e., the year within the “birth” field, and report an array of _ids for each birth year.
print("Question 2.2 aggregation: ");
var temp = db.test.aggregate([{$group:{"_id":{"year":{$year:"$birth"}}, "ids":{$push:"$_id"}}}]);
while(temp.hasNext()){
	printjson(temp.next());
}

// 2.3 Report the document with the smallest and largest _ids. You first need to find the values of the smallest and largest, and then report their documents.
print("Question 2.3 query: ");
printjson(db.test.find().sort({"_id":-1}).limit(1).next());
printjson(db.test.find().sort({"_id":1}).limit(1).next());