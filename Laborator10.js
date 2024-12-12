// Laborator 10 - MONGO1

db["students"].find()

// Exercitiul 6
db.students.find()
    .sort({ "student.lastname": -1 })
    .limit(4)
    .pretty()

// Exercitiul 8
db.students.find(
    {
        $and: [
            { $or: [{ sef: false }, { sef: { $exists: false } }] },
            { "cunostinte.1": "Python" }
        ]
    },
    {
        "student.firstname": 1,
        "student.lastname": 1,
        "cunostinte": 1,
        "_id": 0
    }
).pretty()

// Exercitiul 9
db.students.find(
    {}, 
    {
        "_id": 0, 
        "student.firstname": 1, 
        "cunostinte": "Python",
        "sef": { $exists: false }
    }
).pretty()

// Exercitiul 10
db.students.updateMany(
    { an: 3 },
    { 
        $push: { 
            materii: { 
                $each: [
                    { nume: "EGC", an: 3 }, 
                    { nume: "LFA", an: 3 }
                ] 
            }
        }
    }
)

// Exercitiul 11
db.students.deleteOne({
    "materii.nume": "CN2" 
})

db.students.find({ "materii.nume": "CN2" }).pretty()


// Exercitii extra

// studentii care au repetat macar de 2 ori o materie

db.students.find({
    $or: [
        {
            $and: [
                { an: 4 },
                {
                    $or: [
                        { "materii.an": 1 }, 
                        { "materii.an": 2 } 
                    ]
                }
            ]
        },
        {
            $and: [
                { an: 3 }, 
                { "materii.an": 1 } 
            ]
        }
    ]
}).pretty()
