package neoflix.services;

import neoflix.AppUtils;
import neoflix.Params;
import neoflix.ValidationException;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.NoSuchRecordException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class FavoriteService {

    private DbUtils dbUtils;
    private final Driver driver;

    private final List<Map<String,Object>> popular;
    private final List<Map<String,Object>> users;
    private final Map<String,List<Map<String,Object>>> userFavorites = new HashMap<>();

    /**
     * The constructor expects an instance of the Neo4j Driver, which will be
     * used to interact with Neo4j.
     *
     * @param driver
     */
    public FavoriteService(Driver driver) {
        this.driver = driver;
        this.popular = AppUtils.loadFixtureList("popular");
        this.users = AppUtils.loadFixtureList("users");
        this.dbUtils = new DbUtils();
    }

    /**
     * This method should retrieve a list of movies that have an incoming :HAS_FAVORITE
     * relationship from a User node with the supplied `userId`.
     *
     * Results should be ordered by the `sort` parameter, and in the direction specified
     * in the `order` parameter.
     * Results should be limited to the number passed as `limit`.
     * The `skip` variable should be used to skip a certain number of rows.
     *
     * @param userId  The unique ID of the user
     * @param params Query params for pagination and sorting
     * @return List<Movie> An list of Movie objects
     */
    // tag::all[]
    public List<Map<String, Object>> all(String userId, Params params) {
        // TODO: Open a new session
        // TODO: Retrieve a list of movies favorited by the user
        // TODO: Close session

        try (var session = driver.session()) {
            String statement = String.format("""
                MATCH (u:User {userId: $userId})-[r:HAS_FAVORITE]->(m:Movie)
                RETURN m {
                .*,
                  favorite: true
                } AS movie
                ORDER BY m.`%s` %s
                SKIP $skip
                LIMIT $limit
            """, params.sort(Params.Sort.title), params.order());
            Function<Record, Map<String, Object>> movieMappingFunc = row -> row.get("movie").asMap();
            return this.dbUtils.readAll(session, statement,
                    Map.of("userId", userId, "skip", params.skip(), "limit", params.limit()),
                    movieMappingFunc);
        }
    }
    // end::all[]

    /**
     * This method should create a `:HAS_FAVORITE` relationship between
     * the User and Movie ID nodes provided.
     *
     * If either the user or movie cannot be found, a `NotFoundError` should be thrown.
     *
     * @param userId The unique ID for the User node
     * @param movieId The unique tmdbId for the Movie node
     * @return Map<String,Object></String,Object> The updated movie record with `favorite` set to true
     */
    // tag::add[]
    public Map<String,Object> add(String userId, String movieId) {
        // TODO: Open a new Session
        // TODO: Create HAS_FAVORITE relationship within a Write Transaction
        // TODO: Close the session
        // TODO: Return movie details and `favorite` property
        try (var session = driver.session()) {
            String statement = """
                MATCH (u:User {userId: $userId})
                MATCH (m:Movie {tmdbId: $movieId})

                MERGE (u)-[r:HAS_FAVORITE]->(m)
                        ON CREATE SET r.createdAt = datetime()

                RETURN m {
                    .*,
                    favorite: true
                } AS movie
            """;
            Function<Record, Map<String, Object>> movieMappingFunc = row -> row.get("movie").asMap();
            return this.dbUtils.write(session, statement,
                    Map.of("userId", userId, "movieId", movieId),
                    movieMappingFunc);
        } catch (NoSuchRecordException e) {
            throw new ValidationException(
                    String.format("Couldn't create a favorite relationship for User %s and Movie %s", userId, movieId),
                    Map.of("movie",movieId, "user",userId));
        }
    }
    // end::add[]

    /*
     *This method should remove the `:HAS_FAVORITE` relationship between
     * the User and Movie ID nodes provided.
     * If either the user, movie or the relationship between them cannot be found,
     * a `NotFoundError` should be thrown.

     * @param userId The unique ID for the User node
     * @param movieId The unique tmdbId for the Movie node
     * @return Map<String,Object></String,Object> The updated movie record with `favorite` set to true
     */
    // tag::remove[]
    public Map<String,Object> remove(String userId, String movieId) {
        // TODO: Open a new Session
        // TODO: Delete the HAS_FAVORITE relationship within a Write Transaction
        // TODO: Close the session
        // TODO: Return movie details and `favorite` property
        try (var session = driver.session()) {
            String statement = """
                MATCH (u:User {userId: $userId})-[r:HAS_FAVORITE]->(m:Movie {tmdbId: $movieId})
                DELETE r
                
                RETURN m {
                  .*,
                  favorite: false
                } AS movie
            """;
            Function<Record, Map<String, Object>> movieMappingFunc = row -> row.get("movie").asMap();
            return this.dbUtils.write(session, statement,
                    Map.of("userId", userId, "movieId", movieId),
                    movieMappingFunc);
        } catch (NoSuchRecordException e) {
            throw new ValidationException(
                    String.format("Couldn't create a favorite relationship for User %s and Movie %s", userId, movieId),
                    Map.of("movie",movieId, "user",userId));
        }
    }
    // end::remove[]
}
