import {createClient} from "@clickhouse/client";
import 'dotenv/config';

const client = createClient({
    url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
    username: process.env.CLICKHOUSE_USER || 'default',
    password: process.env.CLICKHOUSE_PASSWORD || '',
    database: process.env.CLICKHOUSE_DB || 'default',
});

const statsUrl = 'https://realraum.at/status.json';

const intervalMs = 10 * 1000; // 10 seconds

const migrations = [
    {
        id: 'create_json_table',
        sql: `
            CREATE TABLE IF NOT EXISTS r3stats.stats
            (
                timestamp DateTime64(6) MATERIALIZED now() CODEC(DoubleDelta, LZ4),
                json String CODEC(ZSTD(1)),

                api String MATERIALIZED JSONExtractString(json, 'api'),

                state_open boolean MATERIALIZED JSONExtractBool(JSONExtractRaw(json, 'state'), 'open'),
                state_lastchange DateTime64(6) MATERIALIZED toDateTime(JSONExtractInt(JSONExtractRaw(json, 'state'), 'lastchange'), 'Europe/Vienna'),

                door_locked Array(String) MATERIALIZED JSONExtractArrayRaw(JSONExtractRaw(json, 'sensors'), 'door_locked'),
                door_contact Array(String) MATERIALIZED JSONExtractArrayRaw(JSONExtractRaw(json, 'sensors'), 'ext_door_ajar'),
                temperature Array(String) MATERIALIZED JSONExtractArrayRaw(JSONExtractRaw(json, 'sensors'), 'temperature'),
                humidity Array(String) MATERIALIZED JSONExtractArrayRaw(JSONExtractRaw(json, 'sensors'), 'humidity'),
                illumination Array(String) MATERIALIZED JSONExtractArrayRaw(JSONExtractRaw(json, 'sensors'), 'ext_illumination'),
                voltage Array(String) MATERIALIZED JSONExtractArrayRaw(JSONExtractRaw(json, 'sensors'), 'ext_voltage'),
                barometer Array(String) MATERIALIZED JSONExtractArrayRaw(JSONExtractRaw(json, 'sensors'), 'barometer'),

                total_member_count UInt64 MATERIALIZED JSONExtractUInt(JSONExtractArrayRaw(JSONExtractRaw(json, 'sensors'), 'total_member_count')[1], 'value')
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(timestamp)
            ORDER BY (timestamp)
            SETTINGS index_granularity = 8192;
        `,
    },
    {
        id: 'add_ttl_to_table',
        sql: `
            ALTER TABLE r3stats.stats
            MODIFY TTL toDateTime(timestamp) + INTERVAL 1 YEAR;
        `,
    }
];

const initializeMetadataTable = async () => {
    await client.exec({
        query: `
            CREATE TABLE IF NOT EXISTS migrations (
                id String,
                applied_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (id);
        `,
    });
};

const applyMigrations = async () => {
    for await (const migration of migrations) {
        const result = await client.query({
            query: `SELECT COUNT(*) AS count FROM migrations WHERE id = {id:String}`,
            format: 'JSONEachRow',
            query_params: {
                id: migration.id,
            },
        });

        const resultJson = await result.json<{ count: string }>();

        const countStr = resultJson[0]?.count;

        if (typeof countStr === 'undefined') {
            throw new Error(`Failed to check migration status for ${migration.id}`);
        }

        const count = parseInt(countStr, 10);

        if (isNaN(count)) {
            throw new Error(`Invalid count value for migration ${migration.id}: ${countStr}`);
        }

        if (count === 0) {
            console.log(`Applying migration: ${migration.id}`);
            try {
                await client.exec({ query: migration.sql });
                await client.exec({
                    query: `INSERT INTO migrations (id) VALUES ({id:String})`,
                    query_params: {
                        id: migration.id,
                    },
                });
            } catch (error) {
                console.error(`Error applying migration ${migration.id}:`, error);
                throw error;
            }
            console.log(`Migration applied: ${migration.id}`);
        } else {
            console.log(`Migration already applied: ${migration.id}`);
        }
    }
};

const setupDatabase = async () => {
    await initializeMetadataTable();
    await applyMigrations();
    console.log('Database setup complete.');
};

const fetchStats = async (): Promise<object | null> => {
    const abortController = new AbortController();

    setTimeout(() => {
        abortController.abort();
    }, intervalMs / 2);

    try {
        const response = await fetch(statsUrl, {
            headers: {
                'User-Agent': 'r3stats-clickhouse-logger/1.0',
            },
            signal: abortController.signal,
        });

        if (!response.ok) {
            throw new Error(`Failed to fetch stats: ${response.status} ${response.statusText}`);
        }

        return await response.json() as object;
    } catch (error) {
        // if ETIMEDOUT, skip logging
        if (error instanceof Error && (error as any).code === 'ETIMEDOUT') {
            console.warn('Fetch stats request timed out, skipping this interval.');
            return null;
        }

        // also do not log abort errors
        if (error instanceof Error && error.name === 'AbortError') {
            console.warn('Fetch stats request aborted due to timeout, skipping this interval.');
            return null;
        }

        console.error('Error fetching stats:', error);
        return null;
    }
};

const fetchAndInsertStats = async () => {
    const stats = await fetchStats();

    if (!stats) {
        console.error('No stats fetched, skipping insertion.');
        return;
    }

    const res = await client.insert({
        table: 'r3stats.stats',
        values: [{
            json: JSON.stringify(stats),
        }],
        format: 'JSONEachRow',
    });

    if (!res.executed) {
        console.error('Failed to insert stats into ClickHouse.', res);
    }
};

const main = async () => {
    await setupDatabase();
    await fetchAndInsertStats();

    // execute every 10 seconds
    setInterval(async () => {
        await fetchAndInsertStats();
    }, intervalMs);
};

main().catch((error) => {
    console.error('Error in main execution:', error);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (error) => {
    console.error('Uncaught Exception thrown:', error);
});
