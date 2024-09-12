// Import the postgres library
import { nanoid } from "nanoid";
import postgres from "postgres";
import pg from "pg";

const url = process.env.DBURL;

const { Client } = pg;
const pool = new pg.Pool({ connectionString: url });
const client = await pool.connect();

const sql = postgres(url, {
  max: 1,
});

async function timed(fn) {
  const start = Date.now();
  try {
    await fn();
  } finally {
    console.log("Took", Date.now() - start, "ms");
  }
}

async function warmUp() {
  // I do not know why but postgres seems to warm up connections slower than pg
  // I imagine this is exacerbated by my extreme distance to db
  // Perhaps this can be fixed?
  console.log(`Warming up postgres - inserting 1 row`);
  await timed(async () => {
    await sql`insert into message values (${nanoid()}, 'default', 'bonk', 'bonk', 1, false, 1)`;
  });

  console.log("Warming up pg - inserting 1 row");
  await timed(async () => {
    await client.query(
      "insert into message values ($1, 'default', 'bonk', 'bonk', 1, false, 1)",
      [nanoid()]
    );
  });
}

async function go() {
  const num = 100;

  console.log(`Inserting ${num} rows in 1 tx using pg`);
  let promises = [];
  await timed(async () => {
    await client.query("begin");
    try {
      for (let i = 0; i < num; i++) {
        console.log("Inserting row", i);
        promises.push(
          client.query(
            "insert into message values ($1, 'default', 'bonk', 'bonk', 1, false, 1)",
            [nanoid()]
          )
        );
      }
      await Promise.all(promises);
    } finally {
      await client.query("commit");
    }
  });

  console.log(`Inserting ${num} rows in 1 tx using postgres`);
  await timed(async () => {
    await sql.begin(async (sql) => {
      for (let i = 0; i < num; i++) {
        console.log("Inserting row", i);
        sql`insert into message values (${nanoid()}, 'default', 'bonk', 'bonk', 1, false, 1)`.execute();
      }
    });
  });
}

await warmUp();
await go();
