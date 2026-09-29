import java.io.PrintStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;

import org.duckdb.DuckDBConnection;

import org.neo4j.batchimport.api.BatchImporter;
import org.neo4j.batchimport.api.InputIterable;
import org.neo4j.batchimport.api.InputIterator;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.batchimport.api.input.InputChunk;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.batchimport.api.input.ReadableGroups;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.batchimport.DefaultAdditionalIds;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.importer.PrintingImportLogicMonitor;
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;

/**
 * Streams nodes/relationships straight from DuckDB into a Neo4j store using the
 * offline BatchImporter engine -- WITHOUT ever writing a CSV file.
 *
 * This is the same engine that `neo4j-admin database import full` drives; here we
 * bypass its CSV front-end and feed a custom {@link Input} instead.
 *
 * Usage: DuckImport <numPersons> <numKnows> [batchSize]
 */
public class DuckImport {

    static final String[] PERSON = {"Person"};
    static final long BATCH = 250_000; // rows per chunk / id-range

    public static void main(String[] args) throws Exception {
        long nPersons = args.length > 0 ? Long.parseLong(args[0]) : 1000;
        long nKnows   = args.length > 1 ? Long.parseLong(args[1]) : 1000;

        Path work   = Path.of("/tmp/duckwork");
        Path duckDb = work.resolve("data.duckdb");
        Path home   = work.resolve("neo4j-home");
        home.toFile().mkdirs();

        // ---------- 1) Generate the dataset INSIDE DuckDB (no CSV) ----------
        String url = "jdbc:duckdb:" + duckDb;
        duckDb.toFile().delete();
        long t0 = System.nanoTime();
        DuckDBConnection root = (DuckDBConnection) java.sql.DriverManager.getConnection(url);
        try (Statement st = root.createStatement()) {
            st.execute("CREATE OR REPLACE TABLE person AS " +
                    "SELECT range AS id, 'n' || range AS name FROM range(0, " + nPersons + ")");
            // s/e stay within [0, nPersons) so they reference real node ids
            st.execute("CREATE OR REPLACE TABLE knows AS " +
                    "SELECT range AS rid, range % " + nPersons + " AS s, (range + 1) % " + nPersons + " AS e " +
                    "FROM range(0, " + nKnows + ")");
        }
        long personCount, knowsCount;
        try (Statement st = root.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT (SELECT count(*) FROM person), (SELECT count(*) FROM knows)");
            rs.next();
            personCount = rs.getLong(1);
            knowsCount  = rs.getLong(2);
        }
        System.out.printf("DuckDB ready: %d person, %d knows (gen %.1fs)%n",
                personCount, knowsCount, (System.nanoTime() - t0) / 1e9);

        // ---------- 2) A streamed Input backed by DuckDB ----------
        InputIterable nodes = () -> new DuckIterator(root, personCount,
                "SELECT id, name FROM person WHERE id >= ? AND id < ?", true);
        InputIterable rels = () -> new DuckIterator(root, knowsCount,
                "SELECT s, e FROM knows WHERE rid >= ? AND rid < ?", false);

        // We supply estimates directly -> NO sampling pass, NO source.length() needed.
        Input.Estimates estimates = Input.knownEstimates(
                personCount,          // number of nodes
                knowsCount,           // number of relationships
                personCount,          // number of node properties (one 'name' each)
                0,                    // number of relationship properties
                personCount * 16,     // node property size on disk (approx)
                0,                    // relationship property size
                personCount);         // number of node labels (one each)

        Input input = Input.input(nodes, rels, IdType.INTEGER, estimates, ReadableGroups.EMPTY);

        // ---------- 3) Drive the BatchImporter (recipe copied from ImportCommand$Full) ----------
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        Config dbConfig = Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, home.toAbsolutePath())
                .build();
        DatabaseLayout layout = Neo4jLayout.of(dbConfig).databaseLayout("neo4j");
        fs.mkdirs(layout.databaseDirectory());
        fs.mkdirs(layout.getTransactionLogsDirectory());

        StorageEngineFactory sef = StorageEngineFactory.selectStorageEngine(dbConfig);
        JobScheduler scheduler = JobSchedulerFactory.createInitialisedScheduler();
        LogService logService = NullLogService.getInstance();
        Monitor monitor = new PrintingImportLogicMonitor(System.out, System.err);

        long t1 = System.nanoTime();
        try {
            BatchImporter importer = sef.batchImporter(
                    layout,
                    fs,
                    PageCacheTracer.NULL,
                    org.neo4j.batchimport.api.Configuration.DEFAULT,
                    logService,
                    System.out,                 // progressOutput -> prints "IMPORT DONE ..."
                    false,                      // verbose
                    DefaultAdditionalIds.EMPTY,
                    dbConfig,
                    monitor,
                    scheduler,
                    Collector.EMPTY,
                    TransactionLogInitializer.getLogFilesInitializer(),
                    new IndexImporterFactoryImpl(),
                    EmptyMemoryTracker.INSTANCE,
                    CursorContextFactory.NULL_CONTEXT_FACTORY);
            importer.doImport(input);
        } finally {
            scheduler.close();
            root.close();
        }
        double secs = (System.nanoTime() - t1) / 1e9;
        long totalEntities = personCount + knowsCount;
        System.out.printf("%nRESULT: %d nodes + %d rels imported in %.1fs  (~%.0f entities/s)  NO CSV%n",
                personCount, knowsCount, secs, totalEntities / secs);
    }

    /** Thread-safe InputIterator: hands out id-ranges atomically. */
    static final class DuckIterator implements InputIterator {
        private final DuckDBConnection root;
        private final long total;
        private final String sql;
        private final boolean isNode;
        private final AtomicLong cursor = new AtomicLong(0);

        DuckIterator(DuckDBConnection root, long total, String sql, boolean isNode) {
            this.root = root; this.total = total; this.sql = sql; this.isNode = isNode;
        }

        @Override public InputChunk newChunk() {
            try {
                Connection c = root.duplicate();      // per-thread connection, same DuckDB instance
                return new DuckChunk(c, c.prepareStatement(sql), isNode);
            } catch (Exception e) { throw new RuntimeException(e); }
        }

        @Override public boolean next(InputChunk chunk) {
            long from = cursor.getAndAdd(BATCH);
            if (from >= total) return false;
            long to = Math.min(from + BATCH, total);
            ((DuckChunk) chunk).load(from, to);
            return true;
        }

        @Override public void close() {}
    }

    /** One chunk = one [from,to) id-range materialised by a DuckDB query. */
    static final class DuckChunk implements InputChunk {
        private final Connection conn;
        private final PreparedStatement ps;
        private final boolean isNode;
        private ResultSet rs;

        DuckChunk(Connection conn, PreparedStatement ps, boolean isNode) {
            this.conn = conn; this.ps = ps; this.isNode = isNode;
        }

        void load(long from, long to) {
            try {
                if (rs != null) rs.close();
                ps.setLong(1, from);
                ps.setLong(2, to);
                rs = ps.executeQuery();
            } catch (Exception e) { throw new RuntimeException(e); }
        }

        @Override public boolean next(InputEntityVisitor v) throws java.io.IOException {
            try {
                if (rs == null || !rs.next()) return false;
                if (isNode) {
                    v.id(rs.getLong(1));
                    v.labels(PERSON);
                    v.property("name", rs.getString(2));
                } else {
                    v.startId(rs.getLong(1));
                    v.endId(rs.getLong(2));
                    v.type("KNOWS");
                }
                v.endOfEntity();
                return true;
            } catch (java.sql.SQLException e) { throw new java.io.IOException(e); }
        }

        @Override public void close() {
            try { if (rs != null) rs.close(); ps.close(); conn.close(); } catch (Exception ignore) {}
        }
    }
}
