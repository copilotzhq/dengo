import { Database, Document } from "./mod.ts";

// Test data interfaces
interface User extends Document {
  name: string;
  email: string;
  age: number;
  tags: string[];
  metadata: {
    lastLogin: Date;
    status: string;
  };
}

// Benchmark utilities
const generateUser = (i: number): Omit<User, '_id'> => ({
  name: `User ${i}`,
  email: `user${i}@example.com`,
  age: 20 + (i % 30),
  tags: [`tag${i % 5}`, `tag${i % 3}`],
  metadata: {
    lastLogin: new Date(),
    status: i % 2 === 0 ? "active" : "inactive"
  }
});

const generateUsers = (count: number): Omit<User, '_id'>[] => 
  Array.from({ length: count }, (_, i) => generateUser(i));

// Setup database and collection
const setupCollection = async () => {
  const kv = await Deno.openKv(":memory:");
  const db = new Database(kv);
  const users = db.collection<User>("users");
  return { kv, users };
};

// Single operation benchmarks
Deno.bench("insertOne - single document", async () => {
  const { kv, users } = await setupCollection();
  try {
    await users.insertOne(generateUser(1));
  } finally {
    kv.close();
  }
});

Deno.bench("findOne - by _id", async () => {
  const { kv, users } = await setupCollection();
  try {
    const doc = await users.insertOne(generateUser(1));
    await users.findOne({ _id: doc.insertedId });
  } finally {
    kv.close();
  }
});

// Batch operation benchmarks
Deno.bench("insertMany - 100 documents", async () => {
  const { kv, users } = await setupCollection();
  try {
    await users.insertMany(generateUsers(100));
  } finally {
    kv.close();
  }
});

// Index operation benchmarks
Deno.bench("createIndex - single field", async () => {
  const { kv, users } = await setupCollection();
  try {
    await users.createIndex("email", { unique: true });
  } finally {
    kv.close();
  }
});

Deno.bench("createIndex - compound", async () => {
  const { kv, users } = await setupCollection();
  try {
    await users.createIndex({
      key: { age: 1, "metadata.status": 1 }
    });
  } finally {
    kv.close();
  }
});

// Query operation benchmarks
Deno.bench("find - with filter", async () => {
  const { kv, users } = await setupCollection();
  try {
    await users.insertMany(generateUsers(100));
    await users.find({ age: { $gte: 25 } });
  } finally {
    kv.close();
  }
});

Deno.bench("find - with sort", async () => {
  const { kv, users } = await setupCollection();
  try {
    await users.insertMany(generateUsers(100));
    await users.find({}, { sort: { age: -1 } });
  } finally {
    kv.close();
  }
});

Deno.bench("find - with projection", async () => {
  const { kv, users } = await setupCollection();
  try {
    await users.insertMany(generateUsers(100));
    await users.find({}, { projection: { name: 1, email: 1 } });
  } finally {
    kv.close();
  }
});

// Complex operation benchmarks
Deno.bench("complex query with index", async () => {
  const { kv, users } = await setupCollection();
  try {
    await users.createIndex("age");
    await users.insertMany(generateUsers(100));
    await users.find({
      age: { $gte: 25, $lte: 35 },
      "metadata.status": "active"
    }, {
      sort: { age: -1 },
      limit: 10
    });
  } finally {
    kv.close();
  }
});

// Update operation benchmarks
Deno.bench("updateOne - simple update", async () => {
  const { kv, users } = await setupCollection();
  try {
    const doc = await users.insertOne(generateUser(1));
    await users.updateOne(
      { _id: doc.insertedId },
      { $set: { age: 40 } }
    );
  } finally {
    kv.close();
  }
});

Deno.bench("updateMany - batch update", async () => {
  const { kv, users } = await setupCollection();
  try {
    await users.insertMany(generateUsers(100));
    await users.updateMany(
      { "metadata.status": "inactive" },
      { $set: { "metadata.status": "active" } }
    );
  } finally {
    kv.close();
  }
});

// Delete operation benchmarks
Deno.bench("deleteOne - by _id", async () => {
  const { kv, users } = await setupCollection();
  try {
    const doc = await users.insertOne(generateUser(1));
    await users.deleteOne({ _id: doc.insertedId });
  } finally {
    kv.close();
  }
});

Deno.bench("deleteMany - with filter", async () => {
  const { kv, users } = await setupCollection();
  try {
    await users.insertMany(generateUsers(100));
    await users.deleteMany({ "metadata.status": "inactive" });
  } finally {
    kv.close();
  }
});

// Indexed vs non-indexed query comparison
Deno.bench("query - without index", async () => {
  const { kv, users } = await setupCollection();
  try {
    await users.insertMany(generateUsers(100));
    await users.find({ age: { $gte: 25 } });
  } finally {
    kv.close();
  }
});

Deno.bench("query - with index", async () => {
  const { kv, users } = await setupCollection();
  try {
    await users.createIndex("age");
    await users.insertMany(generateUsers(100));
    await users.find({ age: { $gte: 25 } });
  } finally {
    kv.close();
  }
}); 