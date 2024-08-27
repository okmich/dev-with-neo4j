package neoflix.services;

import neoflix.AppUtils;
import neoflix.Params;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.TransactionContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class MovieService {
    private final DbUtils dbUtils;

    private final Driver driver;

    /**
     * The constructor expects an instance of the Neo4j Driver, which will be
     * used to interact with Neo4j.
     */
    public MovieService(Driver driver) {
        this.driver = driver;
        this.dbUtils = new DbUtils();
    }

    /**
     * This method should return a paginated list of movies ordered by the `sort`
     * parameter and limited to the number passed as `limit`.  The `skip` variable should be
     * used to skip a certain number of rows.
     *
     * If a userId value is supplied, a `favorite` boolean property should be returned to
     * signify whether the user has aded the movie to their "My Favorites" list.
     *
     * @param params query params (query, sort, order, limit, skip)
     * @param userId
     * @returns {Promise<Record<string, any>[]>}
     */
    // tag::all[]
    public List<Map<String,Object>> all(Params params, String userId) {
        // TODO: Open an Session
        try (var session = driver.session()) {
            var movies = session.executeRead(tx -> {
                // Get an array of IDs for the User's favorite movies
                var favorites = getUserFavorites(tx, userId);
                Params.Sort sort = params.sort(Params.Sort.title);
                String query = String.format("""
                        MATCH (m:Movie)
                        WHERE m.`%s` IS NOT NULL
                        RETURN m {
                          .*,
                          favorite: m.tmdbId IN $favorites
                        } AS movie
                        ORDER BY m.`%s` %s
                        SKIP $skip
                        LIMIT $limit
                    """, sort, sort, params.order());

                return this.dbUtils.run(tx, query, Map.of("skip", params.skip(), "limit", params.limit(), "favorites",favorites),
                        row -> row.get("movie").asMap());
            });
            return movies;
        }
    }
    // end::all[]


    /**
     * @public
     * This method find a Movie node with the ID passed as the `id` parameter.
     * Along with the returned payload, a list of actors, directors, and genres should
     * be included.
     * The number of incoming RATED relationships should also be returned as `ratingCount`
     *
     * If a userId value is suppled, a `favorite` boolean property should be returned to
     * signify whether the user has aded the movie to their "My Favorites" list.
     *
     * @param {string} id
     * @returns {Promise<Record<string, any>>}
     */
    // tag::findById[]
    public Map<String,Object> findById(String id, String userId) {
        try (var session = driver.session()) {
            var result = session.executeRead(tx -> {
                // Get an array of IDs for the User's favorite movies
                var favorites = getUserFavorites(tx, userId);
                String query = String.format("""
                        MATCH (m:Movie {tmdbId: $id})
                        RETURN m {
                          .*,
                          actors: [ (a)-[r:ACTED_IN]->(m) | a { .*, role: r.role } ],
                          directors: [ (d)-[:DIRECTED]->(m) | d { .* } ],
                          genres: [ (m)-[:IN_GENRE]->(g) | g { .name }],
                          ratingCount: count{ (m)<-[:RATED]-() },
                          favorite: m.tmdbId IN $favorites
                        } AS movie
                        LIMIT 1
                    """);

                var res = this.dbUtils.runSingle(tx, query, Map.of("id", id, "favorites",favorites),
                        row -> row.get("movie").asMap());
                return res.orElseThrow(RuntimeException::new);
            });
            return result;
        }
    }
    // end::findById[]

    /**
     * This method should return a paginated list of similar movies to the Movie with the
     * id supplied.  This similarity is calculated by finding movies that have many first
     * degree connections in common: Actors, Directors and Genres.
     *
     * Results should be ordered by the `sort` parameter, and in the direction specified
     * in the `order` parameter.
     * Results should be limited to the number passed as `limit`.
     * The `skip` variable should be used to skip a certain number of rows.
     *
     * If a userId value is suppled, a `favorite` boolean property should be returned to
     * signify whether the user has aded the movie to their "My Favorites" list.
     *
     * @param id
     * @param params Query parameters for pagination and ordering
     * @param userId
     * @returns List<Movie> similar movies
     */
    // tag::getSimilarMovies[]
    public List<Map<String,Object>> getSimilarMovies(String id, Params params, String userId) {
        // TODO: Get similar movies based on genres or ratings
        try (var session = driver.session()) {
            var result = session.executeRead(tx -> {
                // Get an array of IDs for the User's favorite movies
                var favorites = getUserFavorites(tx, userId);
                String query = String.format("""
                    MATCH (:Movie {tmdbId: $id})-[:IN_GENRE|ACTED_IN|DIRECTED]->()<-[:IN_GENRE|ACTED_IN|DIRECTED]-(m)
                    WHERE m.imdbRating IS NOT NULL
                    
                    WITH m, count(*) AS inCommon
                    WITH m, inCommon, m.imdbRating * inCommon AS score
                    ORDER BY score DESC
                    
                    SKIP $skip
                    LIMIT $limit
                    
                    RETURN m {
                       .*,
                       score: score,
                       favorite: m.tmdbId IN $favorites
                    } AS movie
                    """);

                return this.dbUtils.run(tx, query, Map.of("id", id, "skip", params.skip(), "limit", params.limit(), "favorites",favorites),
                        row -> row.get("movie").asMap());
            });
            return result;
        }
    }
    // end::getSimilarMovies[]


    /**
     * This method should return a paginated list of movies that have a relationship to the
     * supplied Genre.
     *
     * Results should be ordered by the `sort` parameter, and in the direction specified
     * in the `order` parameter.
     * Results should be limited to the number passed as `limit`.
     * The `skip` variable should be used to skip a certain number of rows.
     *
     * If a userId value is supplied, a `favorite` boolean property should be returned to
     * signify whether the user has added the movie to their "My Favorites" list.
     *
     * @param name
     * @param params Query parameters for pagination and ordering
     * @param userId
     * @return List<Movie> movies for that genre
     */
    // tag::getByGenre[]
    public List<Map<String,Object>> byGenre(String name, Params params, String userId) {
        try (var session = driver.session()) {
            var movies = session.executeRead(tx -> {
                // Get an array of IDs for the User's favorite movies
                var favorites = getUserFavorites(tx, userId);
                Params.Sort sort = params.sort(Params.Sort.title);
                String query = String.format("""
                        MATCH (m:Movie)-[:IN_GENRE]->(:Genre {name: $name})
                        WHERE m.`%s` IS NOT NULL
                        RETURN m {
                          .*,
                          favorite: m.tmdbId IN $favorites
                        } AS movie
                        ORDER BY m.`%s` %s
                        SKIP $skip
                        LIMIT $limit
                    """, sort, sort, params.order());

                return this.dbUtils.run(tx, query, Map.of("name", name, "skip", params.skip(), "limit", params.limit(), "favorites",favorites),
                        row -> row.get("movie").asMap());
            });
            return movies;
        }
    }
    // end::getByGenre[]

    /**
     * This method should return a paginated list of movies that have an ACTED_IN relationship
     * to a Person with the id supplied
     *
     * Results should be ordered by the `sort` parameter, and in the direction specified
     * in the `order` parameter.
     * Results should be limited to the number passed as `limit`.
     * The `skip` variable should be used to skip a certain number of rows.
     *
     * If a userId value is suppled, a `favorite` boolean property should be returned to
     * signify whether the user has aded the movie to their "My Favorites" list.
     *
     * @param actorId actor id
     * @param params query params with sorting and pagination
     * @param userId
     * @return List<Movie>
     */
    // tag::getForActor[]
    public List<Map<String,Object>> getForActor(String actorId, Params params,String userId) {
        // TODO: Get Movies acted in by a Person
        try (var session = driver.session()) {
            var movies = session.executeRead(tx -> {
                // Get an array of IDs for the User's favorite movies
                var favorites = getUserFavorites(tx, userId);
                Params.Sort sort = params.sort(Params.Sort.title);
                String query = String.format("""
                        MATCH (:Person {tmdbId: $id})-[:ACTED_IN]->(m:Movie)
                        WHERE m.`%s` IS NOT NULL
                        RETURN m {
                          .*,
                          favorite: m.tmdbId IN $favorites
                        } AS movie
                        ORDER BY m.`%s` %s
                        SKIP $skip
                        LIMIT $limit
                    """, sort, sort, params.order());

                return this.dbUtils.run(tx, query, Map.of("id", actorId, "skip", params.skip(), "limit", params.limit(), "favorites",favorites),
                        row -> row.get("movie").asMap());
            });
            return movies;
        }
    }
    // end::getForActor[]

    /**
     * This method should return a paginated list of movies that have an DIRECTED relationship
     * to a Person with the id supplied
     *
     * Results should be ordered by the `sort` parameter, and in the direction specified
     * in the `order` parameter.
     * Results should be limited to the number passed as `limit`.
     * The `skip` variable should be used to skip a certain number of rows.
     *
     * If a userId value is suppled, a `favorite` boolean property should be returned to
     * signify whether the user has aded the movie to their "My Favorites" list.
     *
     * @param directorId director id
     * @param params query params with sorting and pagination
     * @param userId
     * @return List<Movie>
     */
    // tag::getForDirector[]
    public List<Map<String,Object>> getForDirector(String directorId, Params params,String userId) {
        try (var session = driver.session()) {
            var movies = session.executeRead(tx -> {
                // Get an array of IDs for the User's favorite movies
                var favorites = getUserFavorites(tx, userId);
                Params.Sort sort = params.sort(Params.Sort.title);
                String query = String.format("""
                        MATCH (:Person {tmdbId: $id})-[:DIRECTED]->(m:Movie)
                        WHERE m.`%s` IS NOT NULL
                        RETURN m {
                          .*,
                          favorite: m.tmdbId IN $favorites
                        } AS movie
                        ORDER BY m.`%s` %s
                        SKIP $skip
                        LIMIT $limit
                    """, sort, sort, params.order());

                return this.dbUtils.run(tx, query, Map.of("id", directorId, "skip", params.skip(), "limit", params.limit(), "favorites",favorites),
                        row -> row.get("movie").asMap());
            });
            return movies;
        }
    }
    // end::getForDirector[]


    /**
     * This function should return a list of tmdbId properties for the movies that
     * the user has added to their 'My Favorites' list.
     *
     * @param tx The open transaction
     * @param userId The ID of the current user
     * @return List<String> movieIds of favorite movies
     */
    // tag::getUserFavorites[]
    private List<String> getUserFavorites(TransactionContext tx, String userId) {
        if (userId == null) return List.of();
        String query = """
                MATCH (u:User {userId: $userId})-[:HAS_FAVORITE]->(m)
                RETURN m.tmdbId AS id
                """;
        Function<Record, String> func  = row -> row.get("id").asString();
        return this.dbUtils.run(tx, query, Map.of("userId", userId), func);

    }
    // end::getUserFavorites[]

    record Movie() {} // todo
}
