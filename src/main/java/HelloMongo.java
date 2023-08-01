import com.mongodb.MongoException;
import com.mongodb.client.*;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.Position;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.JSONObject;
import org.bson.Document;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import static com.mongodb.client.model.Filters.geoIntersects;

public class HelloMongo {
    public static void main(String args[]) {
        MongoClient mongoClient = null;
        try {
            // Connect to Database
            mongoClient = MongoClients.create("mongodb://localhost:27017");
            MongoDatabase database = mongoClient.getDatabase("mydatabase");
            System.out.println("Connect to MongoDB server successfully");

            // Create restaurants collection and Load Restaurant Data
            MongoCollection<Document> ResCollection = database.getCollection("restaurants");
            MongoCollection<Document> AreaCollection = database.getCollection("neighborhoods");

            // Load if empty
            if(ResCollection.countDocuments()==0) {
                String RestaurantPath = "src/sampleData/restaurants.json";
                LoadDataToDB(database, RestaurantPath,ResCollection);
            }

            if(AreaCollection.countDocuments()==0) {
                String AreaPath = "src/sampleData/neighborhoods.json";
                LoadDataToDB(database, AreaPath,AreaCollection);
            }

            // Query Restaurants in Area
            double[] cord = {40.82302903,-73.93414657};
            double[] coord2 = {40.74654414760227, -73.99870307461414};
            double[] cord3 = {40.8452852288281, -73.93670956526653};
            double[] upperEast = {40.772625926678955, -73.9552630875245};
            double[] Lenox = {40.765839905011674, -73.96287369116266};
            double[] Clinton = {40.76452683974919, -73.99324978961445};
            double[] midTown = {40.75190451065993, -73.98305660602243};
            double[] chelsea = {40.750047403498996, -74.00017532886459};
            Point pos = new Point(new Position(midTown[1], midTown[0]));


            Bson findCurrentArea = geoIntersects("geometry", pos);
            FindIterable<Document> currentNeighborhood = AreaCollection.find(findCurrentArea);

            for (Document doc : currentNeighborhood ){
                System.out.println("You are located at: " + doc.get("name"));
            }

            String jsonString = currentNeighborhood.first().toJson();
            JSONObject doc = new JSONObject(jsonString);
            JSONObject geometry = doc.getJSONObject("geometry");
            JSONArray coordinatesArray = geometry.getJSONArray("coordinates").getJSONArray(0);

            // Normalize JSON
            if(coordinatesArray.length()==1){
                coordinatesArray = coordinatesArray.getJSONArray(0);
            }

            List<Position> coordList = new ArrayList<Position>();

            // Create Position Pairs
            for (int i = 0; i < coordinatesArray.length(); i++) {
                JSONArray point = coordinatesArray.getJSONArray(i);
                double longitude = point.getDouble(0);
                double latitude = point.getDouble(1);
                coordList.add(new Position(longitude,latitude));
//                System.out.println("Longitude: " + longitude + ", Latitude: " + latitude);
            }

            Polygon areaPoly = new Polygon(coordList);

            // GeoFilter using Polygon
            Bson neighborhoodPolygon = geoIntersects("location",areaPoly);
            FindIterable<Document> restaurants = ResCollection.find(neighborhoodPolygon);

            // Display Restaurants in Area
            int count = countDocuments(restaurants);
            System.out.println("Here are the "+count+ " restaurants");
            for (Document doc1 : restaurants ){
                System.out.println("Name: " + doc1.get("name"));
            }
        } catch (MongoException e) {
            System.err.println("Failed to connect to MongoDB server: " + e.getMessage());
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
    }

    private static int countDocuments(FindIterable<Document> restaurants) {
        try (MongoCursor<Document> iterator = restaurants.iterator()){
            int count = 0;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            return count;
        }
    }

    private static void LoadDataToDB(MongoDatabase database, String path, MongoCollection<Document> collection) {
        try (Stream<String> lines = Files.lines(Paths.get(path))) {
            lines.forEach(line -> {
                JSONObject json = new JSONObject(line);
                collection.insertOne(Document.parse(json.toString()));
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
