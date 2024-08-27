from api.data import people, pacino
from api.exceptions.notfound import NotFoundException


class PeopleDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """

    def __init__(self, driver):
        self.driver = driver

    """
    This method should return a paginated list of People (actors or directors),
    with an optional filter on the person's name based on the `q` parameter.

    Results should be ordered by the `sort` parameter and limited to the
    number passed as `limit`.  The `skip` variable should be used to skip a
    certain number of rows.
    """
    # tag::all[]
    def all(self, q, sort = 'name', order = 'ASC', limit = 6, skip = 0):
        def get_all(tx):
            query = f"""
                MATCH (p:Person)
                WHERE $q IS NULL OR p.name CONTAINS $q
                RETURN p {{ .* }} AS person
                ORDER BY p.{sort} {order}
                SKIP $skip
                LIMIT $limit
            """
            results = tx.run(query, q=q, skip=skip, limit=limit)
            return [rec.get("person") for rec in results]
        
        with self.driver.session() as session:
            return session.execute_read(get_all)
    # end::all[]

    """
    Find a user by their ID.

    If no user is found, a NotFoundError should be thrown.
    """
    # tag::findById[]
    def find_by_id(self, id):
        # TODO: Find a user by their ID
        def get_by_id(tx):
            query = """
                MATCH (p:Person {tmdbId: $id})
                RETURN p {
                    .*,
                    actedCount: count { (p)-[:ACTED_IN]->() },
                    directedCount: count { (p)-[:DIRECTED]->() }
                } AS person
            """
            result = tx.run(query, id=id).single()
            if result:
                return result.get("person")
            else:
                raise NotFoundException()
        
        with self.driver.session() as session:
            result = session.execute_read(lambda tx: get_by_id(tx)) 
        return result
    # end::findById[]

    """
    Get a list of similar people to a Person, ordered by their similarity score
    in descending order.
    """
    # tag::getSimilarPeople[]
    def get_similar_people(self, id, limit = 6, skip = 0):
        # TODO: Get a list of similar people to the person by their id
        def get_all(tx):
            query = """
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
            """
            result = tx.run(query, id=id, limit=limit, skip=skip)
            return [row.value("person") for row in result]
        
        with self.driver.session() as session:
            result = session.execute_read(lambda tx: get_all(tx)) 
        return result
    # end::getSimilarPeople[]