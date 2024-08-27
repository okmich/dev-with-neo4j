package neoflix.services;

import neoflix.AppUtils;
import neoflix.AuthUtils;
import neoflix.Params;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Values;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PeopleService {
    private final Driver driver;
    private final DbUtils dbUtils;
    private final List<Map<String,Object>> people;

    /**
     * The constructor expects an instance of the Neo4j Driver, which will be
     * used to interact with Neo4j.
     *
     * @param driver
     */
    public PeopleService(Driver driver) {
        this.driver = driver;
        this.people = AppUtils.loadFixtureList("people");
        this.dbUtils = new DbUtils();
    }

    /**
     * This method should return a paginated list of People (actors or directors),
     * with an optional filter on the person's name based on the `q` parameter.
     *
     * Results should be ordered by the `sort` parameter and limited to the
     * number passed as `limit`.  The `skip` variable should be used to skip a
     * certain number of rows.
     *
     * @param params        Used to filter on the person's name, and query parameters for pagination and ordering
     * @return List<Person>
     */
    // tag::all[]
    public List<Map<String,Object>> all(Params params) {
        try (var session = this.driver.session()){
            String statement = String.format("""
                    MATCH (p:Person)
                    WHERE $q IS null OR p.name CONTAINS $q
                    RETURN p { .* } AS person
                    ORDER BY p.`%s` %s
                    SKIP $skip
                    LIMIT $limit
                    """, params.sort(Params.Sort.name), params.order());
            return this.dbUtils.readAll(session, statement,
                    Map.of("q", Objects.requireNonNullElse(params.query(), ""), "skip", params.skip(), "limit", params.limit()),
                    row -> row.get("person").asMap());
        }
    }
    // end::all[]

    /**
     * Find a user by their ID.
     *
     * If no user is found, a NotFoundError should be thrown.
     *
     * @param id   The tmdbId for the user
     * @return Person
     */
    // tag::findById[]
    public Map<String, Object> findById(String id) {
        // TODO: Find a user by their ID
        try (var session = this.driver.session()){
            String query = """
                MATCH (p:Person {tmdbId: $id})
                          RETURN p {
                            .*,
                            actedCount: count { (p)-[:ACTED_IN]->() },
                            directedCount: count { (p)-[:DIRECTED]->() }
                          } AS person
                """;
            return this.dbUtils.readOne(session, query, Map.of("id", id), row -> row.get("person").asMap())
                    .orElseThrow(() -> new RuntimeException("Persona ID - "+ id+" not found"));
        }
    }
    // end::findById[]

    /**
     * Get a list of similar people to a Person, ordered by their similarity score
     * in descending order.
     *
     * @param id     The ID of the user
     * @param params Query parameters for pagination and ordering
     * @return List<Person> similar people
     */
    // tag::getSimilarPeople[]
    public List<Map<String,Object>> getSimilarPeople(String id, Params params) {
        // TODO: Get a list of similar people to the person by their id
        try (var session = this.driver.session()){
            String query = """
                    MATCH (:Person {tmdbId: $id})-[:ACTED_IN|DIRECTED]->(m)<-[r:ACTED_IN|DIRECTED]-(p)
                    WITH p, collect(m {.tmdbId, .title, type: type(r)}) AS inCommon
                    RETURN p {
                      .*,
                      actedCount: count { (p)-[:ACTED_IN]->() },
                      directedCount: count {(p)-[:DIRECTED]->() },
                      inCommon: inCommon
                    } AS person
                    ORDER BY size(person.inCommon) DESC
                    SKIP $skip
                    LIMIT $limit
                """;
            return this.dbUtils.readAll(session, query,
                    Map.of("id", id, "skip", params.skip(), "limit", params.limit()),
                    row -> row.get("person").asMap());
        }
    }
    // end::getSimilarPeople[]

}