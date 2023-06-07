package org.example;

import com.mongodb.client.*;
import java.io.*;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import java.util.*;
import com.mongodb.client.MongoCollection;
import java.util.logging.Filter;

import static java.util.Collections.singletonList;

public class Main {
    public static MongoClient client = MongoClients.create("mongodb+srv://myAtlasDBUser:Ariana007rings@myatlasclusteredu.1pwbfst.mongodb.net/?retryWrites=true&w=majority");

    public static MongoDatabase database = client.getDatabase("sample_analytics");

    public static MongoCollection collection = database.getCollection("customers");

    public static MongoIterable<String> collectionNames = database.listCollectionNames();

    public static void main(String[] args) {

       DBInfo db = new DBInfo("sample_analytics");
       db.databaseAnalysis();
       db.collectionSize();
        db.indexSize();
        db.unusedIndexes();
        db.unusedCollection();


    }
}