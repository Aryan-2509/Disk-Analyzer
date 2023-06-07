package org.example;
import com.mongodb.client.*;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

public class DBInfo {

    static String dbName;
    MongoClient client;
    MongoDatabase database;
    MongoIterable<String> collectionNames;

    DBInfo(String name)
    {
        dbName = name;
        client = MongoClients.create("mongodb+srv://myAtlasDBUser:Ariana007rings@myatlasclusteredu.1pwbfst.mongodb.net/?retryWrites=true&w=majority");
        database = client.getDatabase(DBInfo.dbName);
        collectionNames = database.listCollectionNames();
    }

    DBInfo(){}

    public void databaseAnalysis()
    {
        System.out.println("Disk Analysis is as follows : ");
        System.out.println();

        for (String collectionName : collectionNames)
        {
            Document stats = database.runCommand(new Document("collStats", collectionName)
                    .append("indexDetails", true));

            //Collection Analysis
            int dataSize_uncompressed = stats.getInteger("size"); //uncompressed
            int storageSize = stats.getInteger("storageSize");
            int freeStorageSize = stats.getInteger("freeStorageSize");

            int storageUsed = storageSize-freeStorageSize; //compressed data
            int dataSize_compressed = storageUsed;
            double compressionEfficiency = (double)(dataSize_uncompressed-dataSize_compressed)/dataSize_uncompressed;

            int numberOfDocuments = stats.getInteger("count");
            int numberOfIndexes = stats.getInteger("nindexes");
            int totalIndexSize = stats.getInteger("totalIndexSize");

            double storageUtilization = ((double)(storageUsed)/storageSize) * 100;

            double paddingFactor = Math.abs((double)(storageSize-dataSize_compressed))/(storageSize);

            int averageObjSize = -1;

            if(stats.getInteger("avgObjSize") != null)
            {
                averageObjSize = stats.getInteger("avgObjSize");
            }
//            int bytesInCache = stats.getInteger("wiredTiger.cache.bytes currently in the cache");
//            int dirtyBytesInCache = stats.getInteger("wiredTiger.cache.bytes dirty in the cache cumulative");
//            int bytesReadIntoCache = stats.getInteger("wiredTiger.cache.bytes read into cache");
//            int bytesWrittenFromCache = stats.getInteger("wiredTiger.cache.bytes written from cache");
//            int pagesFromCache = stats.getInteger("wiredTiger.cache.pages written from cache");
//            int pagesFromMemory = stats.getInteger("wiredTiger.cache.pages written requiring in-memory restoration");
            //double fragmentationFraction = 0.0;
            double indexMemoryCoverage = 0.0;
            double documentMemoryCoverage = 0.0;

            if(dataSize_compressed > 0)
            {
                //fragmentationFraction = (double)storageSize / (double)dataSize_compressed;
                indexMemoryCoverage = ((double)totalIndexSize/(storageSize + dataSize_compressed))*100;
                documentMemoryCoverage = 100 - indexMemoryCoverage;
            }

            System.out.println("Collection name : " + collectionName);
            System.out.println("Total Data Size (uncompressed) : " + dataSize_uncompressed + " bytes");
            System.out.println("Total Data Size (compressed) : " + dataSize_compressed);
            System.out.println("Storage Size: " + storageSize + " bytes");
            System.out.println("Free Storage Size : " + freeStorageSize);
            //System.out.println("Average Object Size: " + averageObjSize + " bytes");
            System.out.println("Number of Documents: " + numberOfDocuments);
            System.out.println("Number of Indexes: " + numberOfIndexes);
            System.out.println("Total Index Size : " + totalIndexSize + " bytes");
            System.out.println("Storage Utilization: " + storageUtilization + " %");
            System.out.println("Compression Efficiency : " + compressionEfficiency + " %");

            if(dataSize_compressed > 0)
            {
                //System.out.println("Fragmentation fraction : " + fragmentationFraction);
                System.out.println("Memory occupied by documents : " + documentMemoryCoverage + " %");
                System.out.println("Memory occupied by indexes : " + indexMemoryCoverage + " %");
            }
            else
            {
                System.out.println("Fragmentation fraction : NA" );
                System.out.println("Memory occupied by documents : NA");
                System.out.println("Memory occupied by indexes : NA" );
            }

            if(averageObjSize != -1)
            {
                System.out.println("Average document size : " + averageObjSize + " bytes");
            }

            System.out.println("Padding Factor : " + paddingFactor);

            System.out.println();

            //Index Analysis
            System.out.println("Index Analysis:");

            Document indexDetails = (Document) stats.get("indexDetails");
            int indexesCount = indexDetails.size();

            System.out.println("Total number of Indexes : " + indexesCount);

            Document indexInsight = (Document) stats.get("indexSizes");

            for(String indexName : indexDetails.keySet())
            {
                int indexSize = indexInsight.getInteger(indexName);
                System.out.println(indexName + " : " + indexSize);
                double indexCoverage = ((double)indexSize/(double)totalIndexSize)*100;
                System.out.println("Index memory coverage : " + indexCoverage + " %");
            }

            System.out.println();
        }
    }

    public void collectionSize()
    {
        ArrayList<Col> collectionArrayList = new ArrayList<Col>();

        System.out.println("Information about various collections is as follows : ");
        System.out.println();

        for (String collectionName : collectionNames) {
            //MongoCollection<Document> current_collection = database.getCollection(collectionName);

            Document command = new Document("collStats", collectionName);
            Document result = database.runCommand(command);

            //System.out.println(result.toJson());

            int collectionSize = result.getInteger("size");
            int count = result.getInteger("count");
            int storageSize = result.getInteger("storageSize");
            int freeStorageSize = result.getInteger("freeStorageSize");

            Col newCollectionObject = new Col(collectionName, storageSize, freeStorageSize);
            collectionArrayList.add(newCollectionObject);

            System.out.println("Collection name : " + collectionName);
            System.out.println("Collection size : " + collectionSize + " bytes");
            System.out.println("Number of Documents : " + count);
            System.out.println();
        }

        System.out.println();

        System.out.println("Free space on the collections are as follows : ");
        Collections.sort(collectionArrayList);

        for (Col temp : collectionArrayList) {
            System.out.println("Collection name : " + temp.name + " Free Space Available : " + temp.freePercent + " %");
        }
        System.out.println();
    }

    public void indexSize()
    {
        System.out.println("Finding size of each index in the database: ");
        System.out.println();

        for (String collectionName : collectionNames) {
            Document command = new Document("collStats", collectionName);
            command.append("indexDetails", true);

            Document result = database.runCommand(command);

            int totalIndexSize = result.getInteger("totalIndexSize");
            Document indexInsight = (Document) result.get("indexSizes");

            System.out.println("Following are the indexes inside the collection " + collectionName + " :");
            System.out.println("Total size of indexes is : " + totalIndexSize);
            System.out.println("Distribution among indexes is as follows : ");
            System.out.println(indexInsight.toJson());
            System.out.println();
        }
        System.out.println();
    }

    public void unusedCollection()
    {
        System.out.println("Finding Unused Collections : ");

        for (String collectionName : collectionNames) {
            Document command = new Document("collStats", collectionName);
            Document result = database.runCommand(command);
            int numberOfDocuments = result.getInteger("count");

            if (numberOfDocuments == 0) {
                System.out.println(collectionName + " is an unused collection");
            }
        }
        System.out.println();
    }

    public void unusedIndexes()
    {
        System.out.println("Finding unused indexes");
        System.out.println();

        for(String collectionName : collectionNames)
        {
            MongoCollection collection = database.getCollection(collectionName);
            MongoIterable<Document> indexStats = collection.aggregate( Arrays.asList(new Document("$indexStats", new Document())));

            System.out.println("Collection name : " + collectionName);
            System.out.println("Unused indexes are : ");

            for(Document doc : indexStats)
            {
                String indexName = doc.getString("name");
                Document temp1 = (Document) doc.get("accesses");
                long ops = temp1.getLong("ops");
                Date date = temp1.getDate("since");

                if(ops == 0)
                {
                    System.out.println(indexName + " : unused since " + date);
                }
            }
            System.out.println();
        }
    }
}
