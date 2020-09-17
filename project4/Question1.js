db.test.insertMany([
   {
       "_id" : 1,
       "name" : {
           "first" : "John",
           "last" : "Backus"
       },
       "birth" : ISODate("1924-12-03T05:00:00Z"),
       "death" : ISODate("2007-03-17T04:00:00Z"),
       "contribs" : [
           "Fortran",
           "ALGOL",
           "Backus-Naur Form",
           "FP"
       ],
       "awards" : [
           {
               "award" : "W.W. McDowell Award",
               "year" : 1967,
               "by" : "IEEE Computer Society"
           },
           {
               "award" : "National Medal of Science",
               "year" : 1975,
               "by" : "National Science Foundation"
           },
           {
               "award" : "Turing Award",
               "year" : 1977,
               "by" : "ACM"
           },
           {
               "award" : "Draper Prize",
               "year" : 1993,
               "by" : "National Academy of Engineering"
           }
       ]
   },
   {
       "_id" : ObjectId("51df07b094c6acd67e492f41"),
       "name" : {
           "first" : "John",
           "last" : "McCarthy"
       },
       "birth" : ISODate("1927-09-04T04:00:00Z"),
       "death" : ISODate("2011-12-24T05:00:00Z"),
       "contribs" : [
           "Lisp",
           "Artificial Intelligence",
           "ALGOL"
       ],
       "awards" : [
           {
               "award" : "Turing Award",
               "year" : 1971,
               "by" : "ACM"
           },
           {
               "award" : "Kyoto Prize",
               "year" : 1988,
               "by" : "Inamori Foundation"
           },
           {
               "award" : "National Medal of Science",
               "year" : 1990,
               "by" : "National Science Foundation"
           }
       ]
   },
   {
       "_id" : 3,
       "name" : {
           "first" : "Grace",
           "last" : "Hopper"
       },
       "title" : "Rear Admiral",
       "birth" : ISODate("1906-12-09T05:00:00Z"),
       "death" : ISODate("1992-01-01T05:00:00Z"),
       "contribs" : [
           "UNIVAC",
           "compiler",
           "FLOW-MATIC",
           "COBOL"
       ],
       "awards" : [
           {
               "award" : "Computer Sciences Man of the Year",
               "year" : 1969,
               "by" : "Data Processing Management Association"
           },
           {
               "award" : "Distinguished Fellow",
               "year" : 1973,
               "by" : " British Computer Society"
           },
           {
               "award" : "W. W. McDowell Award",
               "year" : 1976,
               "by" : "IEEE Computer Society"
           },
           {
               "award" : "National Medal of Technology",
               "year" : 1991,
               "by" : "United States"
           }
       ]
   },
   {
       "_id" : 4,
       "name" : {
           "first" : "Kristen",
           "last" : "Nygaard"
       },
       "birth" : ISODate("1926-08-27T04:00:00Z"),
       "death" : ISODate("2002-08-10T04:00:00Z"),
       "contribs" : [
           "OOP",
           "Simula"
       ],
       "awards" : [
           {
               "award" : "Rosing Prize",
               "year" : 1999,
               "by" : "Norwegian Data Association"
           },
           {
               "award" : "Turing Award",
               "year" : 2001,
               "by" : "ACM"
           },
           {
               "award" : "IEEE John von Neumann Medal",
               "year" : 2001,
               "by" : "IEEE"
           }
       ]
   },
   {
       "_id" : 5,
       "name" : {
           "first" : "Ole-Johan",
           "last" : "Dahl"
       },
       "birth" : ISODate("1931-10-12T04:00:00Z"),
       "death" : ISODate("2002-06-29T04:00:00Z"),
       "contribs" : [
           "OOP",
           "Simula"
       ],
       "awards" : [
           {
               "award" : "Rosing Prize",
               "year" : 1999,
               "by" : "Norwegian Data Association"
           },
           {
               "award" : "Turing Award",
               "year" : 2001,
               "by" : "ACM"
           },
           {
               "award" : "IEEE John von Neumann Medal",
               "year" : 2001,
               "by" : "IEEE"
           }
       ]
   },
   {
       "_id" : 6,
       "name" : {
           "first" : "Guido",
           "last" : "van Rossum"
       },
       "birth" : ISODate("1956-01-31T05:00:00Z"),
       "contribs" : [
           "Python"
       ],
       "awards" : [
           {
               "award" : "Award for the Advancement of Free Software",
               "year" : 2001,
               "by" : "Free Software Foundation"
           },
           {
               "award" : "NLUUG Award",
               "year" : 2003,
               "by" : "NLUUG"
           }
       ]
   },
   {
       "_id" : ObjectId("51e062189c6ae665454e301d"),
       "name" : {
           "first" : "Dennis",
           "last" : "Ritchie"
       },
       "birth" : ISODate("1941-09-09T04:00:00Z"),
       "death" : ISODate("2011-10-12T04:00:00Z"),
       "contribs" : [
           "UNIX",
           "C"
       ],
       "awards" : [
           {
               "award" : "Turing Award",
               "year" : 1983,
               "by" : "ACM"
           },
           {
               "award" : "National Medal of Technology",
               "year" : 1998,
               "by" : "United States"
           },
           {
               "award" : "Japan Prize",
               "year" : 2011,
               "by" : "The Japan Prize Foundation"
           }
       ]
   },
   {
       "_id" : 8,
       "name" : {
           "first" : "Yukihiro",
           "aka" : "Matz",
           "last" : "Matsumoto"
       },
       "birth" : ISODate("1965-04-14T04:00:00Z"),
       "contribs" : [
           "Ruby"
       ],
       "awards" : [
           {
               "award" : "Award for the Advancement of Free Software",
               "year" : "2011",
               "by" : "Free Software Foundation"
           }
       ]
   },
   {
       "_id" : 9,
       "name" : {
           "first" : "James",
           "last" : "Gosling"
       },
       "birth" : ISODate("1955-05-19T04:00:00Z"),
       "contribs" : [
           "Java"
       ],
       "awards" : [
           {
               "award" : "The Economist Innovation Award",
               "year" : 2002,
               "by" : "The Economist"
           },
           {
               "award" : "Officer of the Order of Canada",
               "year" : 2007,
               "by" : "Canada"
           }
       ]
   },
   {
       "_id" : 10,
       "name" : {
           "first" : "Martin",
           "last" : "Odersky"
       },
       "contribs" : [
           "Scala"
       ]
   }
]);

db.test.insert({
    "_id" : 20,
    "name" : {"first" : "Alex", "last" : "Chen" },
    "birth" : ISODate("1933-08-27T04:00:00Z"), 
    "death" : ISODate("1984-11-07T04:00:00Z"), 
    "contribs" : ["C++", "Simula"],
    "awards" : [ {"award" : "WPI Award", "year" : 1977,"by" : "WPI"} ]
});

db.test.insert({
    "_id" : 30, 
    "name" : {"first" : "David","last" : "Mark" },
    "birth" : ISODate("1911-04-12T04:00:00Z"), 
    "death" : ISODate("2000-11-07T04:00:00Z"), 
    "contribs" : ["C++", "FP", "Lisp"],
    "awards" : [{"award" : "WPI Award", "year" : 1963,"by" : "WPI"}, 
                {"award": "Turing Award","year":1996,"by":"ACM"} ]
});
print("Question 1.1 insertion done. "); 

// 1.2 Report all documents of people who got less than 3 awards or have contribution in “FP”.
print("Question 1.2 result: ");
var temp = db.test.find({$or:[{"contribs":"FP"}, {"awards":{$lt:{$size:3}}}]});
while(temp.hasNext()){printjson(temp.next());}
// 1.3 Update the document of “Guido van Rossum” to add “OOP” to the contribution list.
print("Question 1.3 update: ");

db.test.update(
    {"name.first": "Guido", "name.last":"van Rossum"},
    {$push:{"contribs":"OOP"}}
);

// 1.4 Insert a new filed of type array, called “comments”, into the document of “Alex Chen” storing the following comments: “He taught in 3 universities”, “died from cancer”, “lived in CA”.
print("Question 1.4 update: ");

db.test.update(
    {"name.first": "Alex", "name.last":"Chen"},
    {$set:{"comments":["He taught in 3 universities", "died from cancer", "lived in CA"]}}
);

// 1.5 For each contribution by “Alex Chen”, say X, list the peoples’ names (first and last) who have contribution X. E.g., Alex Chen has two contributions in “C++” and “Simula”. Then the output should be like:

print("Question 1.5 query: ");

var contributions = db.test.findOne({"name.first": "Alex", "name.last": "Chen"}).contribs;

for (var i=0; i<contributions.length; i++){
    var c = contributions[i];
    var people = db.test.find({"contribs":c},{"name.first":1, "name.last":1, "_id":0}).toArray();
    var result = {Contribution: c, People: people};
    printjson(result);
}


// 1.6 Report the distinct organization that gave awards. This information can be found in the “by” field inside the “awards” array. The output should be an array of the distinct values, e.g., [“wpi’, “acm’, ...]
print("Question 1.6 query: ");

print(db.test.distinct("awards.award"));

// 1.7 Delete from all documents any award given on 2011.
print("Question 1.7 deletion: ");

db.test.update({},{$pull:{"awards":{"year":2011}}},{ multi: true })
print("Deleted all awards in 2011. ")
// 1.8 Report only the names (first and last) of those individuals who won at least two awards in 2001.
print("Question 1.8 query: ");

var temp = db.test.aggregate([
    {$unwind:"$awards"},
    {$group:{"_id": {"name":{$concat:["$name.first"," ","$name.last"]}, "year":"$awards.year"}, "count":{$sum:1}}},
    {$match:{"count":{$gte:2}}},
    {$project:{"count":0,"_id.year":0}}
    ]);
while(temp.hasNext()){printjson(temp.next());}

// 1.9 Report the document with the largest id. First, you need to find the largest _id (using a CRUD statement), and then use that to report the corresponding document.
print("Question 1.9 query: ");

printjson(db.test.find().sort({"_id":-1}).limit(1).next());

// 1.10 Report only one document where one of the awards is given by “ACM”.
print("Question 1.10 query: ");

printjson(db.test.findOne({"awards.by":"ACM"}));
