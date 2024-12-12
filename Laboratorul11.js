// Ex. 3. Să se găsească toate documentele care se află într-un dreptunghi 
// dat de punctele [20, -100] și [40, 90], care să conțină cuvintele tech 
// și engineering. Cererea să se folosească de indexul textual. Afișeți câmpul rawText. 
// Să se refacă cererea utilizând alt câmp din document.

// Cererea să se folosească de indexul textual
db.documents.createIndex({ "geoLocation": "2d" }) 
db.documents.createIndex({ "lemmaText": "text" }) 

// Afișeți câmpul rawText.
db.documents.find(
    {
        $and: [
            { geoLocation: { $geoWithin: { $box: [[20, -100], [40, 90]] } } },
            { $text: { $search: "tech engineering", $language: "english" } }
        ]
    },
    { rawText: 1, _id: 0 }
)

// Să se refacă cererea utilizând alt câmp din document.
db.documents.find(
    {
        $and: [
            { geoLocation: { $geoWithin: { $box: [[20, -100], [40, 90]] } } },
            { $text: { $search: "tech engineering", $language: "english" } }
        ]
    },
    { summary: 1, _id: 0 }
)

// Funcția tokenization
// Să se scrie o funcție care primește un query pentru filtrare și împarte 
// lemmaText în cuvinte. Funcția va întoarce un vector.

// Funcția tokenization
tokenization = function(q) {
    // Selectam documentele filtrate pe baza query-ului q
    var cursor = db.documents.find(q, { "_id": 0, lemmaText: 1 });
    var tokens = []; // Vector pentru stocarea cuvintelor

    // Parcurgem fiecare document si separam cuvintele
    cursor.forEach(function(elem) {
        if (elem["lemmaText"]) {
            tokens = tokens.concat(elem["lemmaText"].split(" "));
        }
    });

    return tokens; 
};

var q = { gender: "male" };
var tokens = tokenization(q);
print("Tokens:", tokens);


// Ex. 5. Să se scrie o funcție numită countWords care primește un query 
// pentru filtrare și numără aparițiile unui cuvânt. Funcția va întoarce 
// un obiect de forma {word_1: count, word_2: count, …}. Folosiți funcția tokenization.
// Funcția countWords
countWords = function(q) {
    // Obtinem vectorul de cuvinte folosind tokenization
    var tokens = tokenization(q);
    var wordCounts = {}; // Obiect pentru numararea aparitiilor

    // Numaram fiecare cuvant
    tokens.forEach(function(word) {
        if (wordCounts[word]) {
            wordCounts[word]++;
        } else {
            wordCounts[word] = 1;
        }
    });

    return wordCounts; 
};

var q = { gender: "male" }; 
var wordCounts = countWords(q);
print("Word Counts:", wordCounts);


// Ex. 6. Salvați și apoi încărcați în sesiune funcția countWords.
db.system.js.insertOne({
    _id: "countWords",
    value: function(q) {
        var cursor = db.documents.find(q, {"_id": 0, lemmaText: 1});
        var tokens = [];
        var wordCounts = {};

        cursor.forEach(function(elem) {
            tokens = tokens.concat(elem["lemmaText"].split(" "));
        });

        tokens.forEach(function(word) {
            if (wordCounts[word]) {
                wordCounts[word]++;
            } else {
                wordCounts[word] = 1;
            }
        });

        return wordCounts;
    }
});

// ​Ex. 7. Afișați toate cuvintele distincte. Folosiți câmpul words.
db.documents.distinct("words");

// Ex. 8. Să se utilizeze Aggregation Pipeline pentru a calcula numărul de apariții ale cuvintelor.
db.documents.aggregate([
    { $project: { words: { $split: ["$lemmaText", " "] } } },
    { $unwind: "$words" },
    { $group: { _id: "$words", count: { $sum: 1 } } },
    { $sort: { count: -1 } }
]);

// Ex. 9. Să se utilizeze Aggregation Pipeline pentru a calcula numărul de apariții ale cuvintelor folosind coloana words.
db.documents.aggregate([
    { $unwind: "$words" },
    { $group: { _id: "$words", count: { $sum: 1 } } },
    { $sort: { count: -1 } }
]);

// Ex. 10. Să se utilizeze MapReduce pentru a calcula numărul de apariții ale cuvintelor.
var mapFunction = function() {
    var tokens = this.lemmaText.split(" ");
    tokens.forEach(function(token) {
        emit(token, 1);
    });
};

var reduceFunction = function(key, values) {
    return Array.sum(values);
};

db.documents.mapReduce(
    mapFunction,
    reduceFunction,
    { out: "wordCounts" }
);

db.wordCounts.find();

// Ex. 11. Să se utilizeze MapReduce pentru a calcula numărul de apariții ale cuvintelor folosind câmpul words.
var mapFunction = function() {
    this.words.forEach(function(word) {
        emit(word, 1);
    });
};

var reduceFunction = function(key, values) {
    return Array.sum(values);
};

db.documents.mapReduce(
    mapFunction,
    reduceFunction,
    { out: "wordCountsWords" }
);

db.wordCountsWords.find();
