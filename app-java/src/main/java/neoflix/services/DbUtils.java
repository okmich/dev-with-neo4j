package neoflix.services;

import neoflix.Params;
import neoflix.ValidationException;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.NoSuchRecordException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class DbUtils {

    public <T> List<T> run(TransactionContext tx, String cypherQuery, Map<String, Object> paramMap, Function<Record, T> mappingFunc) {
        return tx.run(cypherQuery, paramMap).list(mappingFunc);
    }

    public <T> Optional<T> runSingle(TransactionContext tx, String cypherQuery, Map<String, Object> paramMap, Function<Record, T> mappingFunc) {
        try {
            var record =  tx.run(cypherQuery, paramMap).single();
            return Optional.of(mappingFunc.apply(record));
        } catch (NoSuchRecordException ex){
            return Optional.empty();
        }
    }

    public <T> List<T> readAll(Session session, String cypherQuery, Function<Record, T> mappingFunc) {
        return session.executeRead(tx -> {
            return tx.run(cypherQuery).list(mappingFunc);
        });
    }

    public <T> List<T> readAll(Session session, String cypherQuery, Params params, Params.Sort sort, Function<Record, T> mappingFunc) {
        return session.executeRead(tx -> {
            String query = String.format(cypherQuery, sort, sort, params.order());
            Result res = tx.run(query, Values.parameters("skip", params.skip(), "limit", params.limit()));
            return res.list(mappingFunc);
        });
    }

    public <T> List<T> readAll(Session session, String cypherQuery, Map<String, Object> paramMap, Function<Record, T> mappingFunc) {
        return session.executeRead(tx -> {
            Result res = tx.run(cypherQuery, paramMap);
            return res.list(mappingFunc);
        });
    }

    public <T> Optional<T> readOne(Session session, String cypherQuery, Map<String, Object> valueMap, Function<Record, T> mappingFunc) {
        try {
            var record = session.executeRead(tx -> tx.run(cypherQuery, valueMap).single());
            return Optional.of(mappingFunc.apply(record));
        } catch (NoSuchRecordException ex){
            return Optional.empty();
        }
    }


    public <T> T write(Session session, String cypherQuery, Map<String, Object> valueMap, Function<Record, T> mappingFunc) {
        return session.executeWrite(tx -> {
            var res = tx.run(cypherQuery, valueMap);
            return mappingFunc.apply(res.single());
        });
    }

}
