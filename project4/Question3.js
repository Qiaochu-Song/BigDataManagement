// 3. a parent referencing book collection
db.book1.insertMany([
    {"_id":"Books", "parent":null},
    {"_id":"Programming", "parent":"Books"},
    {"_id":"Languages", "parent": "Programming"},
    {"_id":"Databases", "parent": "Programming"},
    {"_id":"MongoDB", "parent": "Databases"},
    {"_id":"dbm", "parent": "Databases"}
]);

// 3.1 Assume parent-Referencing model, write a query to report the ancestors of “MongoDB”. 
// Formatted into [{Name: "Databases","Level":1},{...}].

var current = "MongoDB";
var ancestors = [];
var i = 1;
while(true){
    var p = db.book1.findOne({"_id":current}, {"parent":1,"_id":0});
    current = p.parent;
    if (current !== null){
        ancestors.push({"Name":current, "Level":i});
        i++;
    } else {break;}
}
print("3.1 ancestors of MongoDB:");
printjson(ancestors);
//ancestors;

// 3.2 Assume parent-Referencing model. You are given only the root node, i.e., _id = “Books”, write a query that reports the height of the tree. 
// (It should be 4 in our case).
var root = db.book1.findOne({"_id":"Books"});
var current = [root._id];
var depth = 1;
while(true){
    var cursor = db.book1.find({"parent":{$in:current}},{"_id":1});
    if (cursor.size()==0){break;}
    else{
        depth ++;
        var current = [];
        while(cursor.hasNext()) {current.push(cursor.next()._id);}
    }
}
print("3.2 depth",depth);

// A child-referencing book collection
db.book2.insertMany([
    {"_id":"Books", "children":["Programming"]},
    {"_id":"Programming", "children":["Languages","Databases"]},
    {"_id":"Languages", "children": []},
    {"_id":"Databases", "children": ["MongoDB", "dbm"]},
    {"_id":"MongoDB", "children": []},
    {"_id":"dbm", "children": []}
]);


// 3.3 Assume Child-Referencing model. Write a query to report the parent of “dbm”.
print("3.3 parent of dbm");
db.book2.findOne({"children":"dbm"})

// 3.4 Assume Child-Referencing model. Write a query to report the descendants of “Books”. The output should be an array containing values [“Programming”, “Languages”, “Databases”, “MongoDB”, “dbm”]

var desc = [];
var stack = [];
stack.push("Books");
while(stack.length>0) {
    var current = stack.shift();
    var nextGen = db.book2.findOne({"_id":current});
    if (nextGen.children.length>0){
        desc = desc.concat(nextGen.children);
        stack = stack.concat(nextGen.children);
    }
}
print("3.4 descendants of Books:", desc);
