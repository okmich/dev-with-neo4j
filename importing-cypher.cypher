// DELETE relationships
MATCH (Person)-[r:ACTED_IN]->(Movie) DELETE r;
MATCH (Person)-[r:DIRECTED]->(Movie) DELETE r;

// delete nodes
MATCH (p:Person) DELETE p;
MATCH (m:Movie) DELETE m;

//Alternatively, you can use DETACH DELETE to delete the nodes and relationships at the same time:
MATCH (p:Person) DETACH DELETE p;
MATCH (m:Movie) DETACH DELETE m;

// rop the constraints on the Person and Movie nodes if they exist:
DROP CONSTRAINT Person_tmdbId IF EXISTS;
DROP CONSTRAINT Movie_movieId IF EXISTS;

CALL {
	LOAD CSV WITH HEADERS
	FROM 'https://data.neo4j.com/importing-cypher/persons.csv' AS row
	MERGE (p:Person {tmdbId: toInteger(row.person_tmdbId)})
	SET
	p.imdbId = toInteger(row.person_imdbId),
	p.bornIn = row.bornIn,
	p.name = row.name,
	p.bio = row.bio,
	p.poster = row.poster,
	p.url = row.url,
	p.born = date(row.born),
	p.died = date(row.died);
} IN TRANSACTIONS OF 100 ROWS;


match (p:Person) return p limit 23;

CREATE CONSTRAINT Person_tmdbId
FOR (x:Person) 
REQUIRE x.tmdbId IS UNIQUE;

CREATE CONSTRAINT Movie_movieId if not exists
FOR (x:Movie) 
REQUIRE x.movieId IS UNIQUE;


LOAD CSV WITH HEADERS
FROM 'https://data.neo4j.com/importing-cypher/movies.csv' AS row
merge (m:Movie {movieId:toInteger(row.movieId)})
set 
	m.movieId=toInteger(row.movieId),
	m.title=row.title,
	m.budget = toInteger(row.budget),
	m.countries = split(row.countries, '|'),
	m.imdbId = toInteger(row.movie_imdbId),
	m.imdbRating=toFloat(row.imdbRating),
	m.runtime = toInteger(row.runtime),
	m.imdbVotes = toInteger(row.imdbVotes),
	m.languages=split(row.languages, '|'),
	m.plot=row.plot,
	m.movie_poster=row.movie_poster,
	m.released=date(row.released),
	m.revenue=row.revenue,
	m.runtime=row.runtime,
	m.movie_tmdbId=row.movie_tmdbId,
	m.movie_url=row.movie_url,
	m.year=toInteger(row.year),
	m.genres=row.genres;

// load ACTED_IN
LOAD CSV WITH HEADERS
FROM 'https://data.neo4j.com/importing-cypher/acted_in.csv' AS row
MATCH (p:Person {tmdbId: toInteger(row.person_tmdbId)})
MATCH (m:Movie {movieId: toInteger(row.movieId)})
MERGE (p)-[r:ACTED_IN]->(m)
SET r.role = row.role;



// load DIRECTED
LOAD CSV WITH HEADERS
FROM 'https://data.neo4j.com/importing-cypher/directed.csv' AS row
MATCH (p:Person {tmdbId: toInteger(row.person_tmdbId)})
MATCH (m:Movie {movieId: toInteger(row.movieId)})
MERGE (p)-[r:DIRECTED]->(m)
SET r.role = row.role;



// adding label
MATCH (p:Person)-[:ACTED_IN]->()
WITH DISTINCT p SET p:Actor;

MATCH (p:Person)-[:DIRECTED]->()
WITH DISTINCT p SET p:Actor;


// multiple passes
//Queries with multiple operations chained together have the potential to write data and then read data that is out of sync - which can result in an Eager operator.
// The Eager operator will cause any operations to execute in their entirety before continuing, ensuring isolation between the different parts of the query. When importing data the Eager operator can cause high memory usage and performance issues.
// A mechanism for avoiding the Eager operator is to break the import into smaller parts. By taking multiple passes over the data file, the query also becomes simpler to understand and change to fit the data model.
LOAD CSV WITH HEADERS
FROM 'https://data.neo4j.com/importing-cypher/books.csv'
AS row
MERGE (b:Book {id: row.id})
SET b.title = row.title
MERGE (a:Author {name: row.author})
MERGE (a)-[:WROTE]->(b);

