package upgrad;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Projections.exclude;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Projections.fields;

public class CRUDHelper {

    /**
     * Display ALl products
     * @param collection
     */
    public static void displayAllProducts(MongoCollection<Document> collection) {
        System.out.println("------ Displaying All Products ------");
        for (Document document : collection.find()) {
            PrintHelper.printSingleCommonAttributes(document);
        }
    }

    /**
     * Display top 5 Mobiles
     * @param collection
     */
    public static void displayTop5Mobiles(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Top 5 Mobiles ------");
        Bson bsonFilter = Filters.eq("Category", "Mobiles");
        for (Document document : collection.find(bsonFilter).limit(5)) {
            PrintHelper.printAllAttributes(document);
        }
    }

    /**
     * Display products ordered by their categories in Descending order without auto generated Id
     * @param collection
     */
    public static void displayCategoryOrderedProductsDescending(MongoCollection<Document> collection) {
        List<String> li = new ArrayList<String>();
        li.add("_id");
        System.out.println("------ Displaying Products ordered by categories ------");
        for (Document document : collection.find().projection(fields(exclude(li))).sort(descending("Category"))) {
            PrintHelper.printAllAttributes(document);
        }
    }


    /**
     * Display number of products in each group
     * @param collection
     */
    public static void displayProductCountByCategory(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Product Count by categories ------");
        for(Document document : collection.aggregate(Arrays.asList(
                Aggregates.group("$Category",
                        Accumulators.sum("Count", 1))
        ))){
            PrintHelper.printProductCountInCategory(document);
        }
    }

    /**
     * Display Wired Headphones
     * @param collection
     */
    public static void displayWiredHeadphones(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Wired headphones ------");
        Bson bsonFilter = Filters.and(Filters.eq("Category", "Headphones"), Filters.eq("ConnectorType", "Wired"));
        for (Document document : collection.find(bsonFilter)){
            PrintHelper.printAllAttributes(document);
        }

    }
}