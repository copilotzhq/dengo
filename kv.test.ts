import { assertEquals, assertRejects, assert } from "jsr:@std/assert";
import { Collection, ObjectId, Document } from "./kv.ts";

interface BaseTestDoc extends Document {
  name: string;
  age: number;
  email?: string;
  status?: string | null;  // Allow null values
  createdAt?: Date;
  tags: string[];
  nested?: { 
    x: number; 
    y?: number;
    labels?: string[];  // Add labels field
  };
  counter?: number;
  [key: string]: unknown;
}

type TestDoc = BaseTestDoc;

Deno.test("Collection.insertOne", async (t) => {
  // Setup - create a new collection before each test
  using kv = await Deno.openKv(":memory:"); // Use in-memory database for tests
  const collection = new Collection(kv, "test_collection");
  
  await t.step("basic document insertion", async () => {
    const doc = { name: "test", value: 123 } as const;
    const result = await collection.insertOne(doc);
    
    // Check that the document was inserted and has an ObjectId
    assert(result.insertedId instanceof ObjectId);
    
    // Verify the document exists in the collection
    const inserted = await collection.findOne({ _id: result.insertedId });
    assertEquals(inserted?.name, doc.name);
    assertEquals(inserted?.value, doc.value);
  });

  await t.step("document with custom _id", async () => {
    const customId = new ObjectId();
    const doc = { _id: customId, name: "test" };
    const result = await collection.insertOne(doc);
    
    assert(result.insertedId.equals(customId));
    const inserted = await collection.findOne({ _id: customId });
    assert(inserted !== null); // First verify document exists
    assert(inserted._id.equals(customId)); // Now TypeScript knows _id exists
  });

  await t.step("duplicate _id handling", async () => {
    const customId = new ObjectId();
    const doc = { _id: customId, name: "test" };
    await collection.insertOne(doc);
    
    await assertRejects(
      async () => {
        await collection.insertOne(doc);
      },
      Error,
      "Duplicate key error"
    );
  });

  await t.step("invalid document handling", async () => {
    await assertRejects(
      async () => {
        await collection.insertOne(null as any);
      },
      Error,
      "Invalid document"
    );

    await assertRejects(
      async () => {
        await collection.insertOne(undefined as any);
      },
      Error,
      "Invalid document"
    );
  });

  await t.step("empty document handling", async () => {
    const result = await collection.insertOne({} as any);
    assert(result.insertedId instanceof ObjectId);
    
    const inserted = await collection.findOne({ _id: result.insertedId });
    assertEquals(Object.keys(inserted || {}).length, 1); // Should only contain _id
  });

  await t.step("special field types handling", async () => {
    interface TestDoc {
      _id?: ObjectId;
      nullField: null;
      boolField: boolean;
      numberField: number;
      dateField: Date;
      arrayField: number[];
      nestedField: { a: number; b: number };
    }

    const doc = {
      nullField: null,
      boolField: true,
      numberField: 123.45,
      dateField: new Date(),
      arrayField: [1, 2, 3],
      nestedField: { a: 1, b: 2 },
    };
    
    const result = await collection.insertOne(doc);
    const inserted = await collection.findOne({ _id: result.insertedId }) as unknown as TestDoc;
    
    // Now TypeScript knows the types
    assertEquals(inserted.nullField, null);
    assertEquals(inserted.boolField, true);
    assertEquals(inserted.numberField, 123.45);
    assertEquals(inserted.dateField.getTime(), doc.dateField.getTime());
    assertEquals(inserted.arrayField, [1, 2, 3]);
    assertEquals(inserted.nestedField, { a: 1, b: 2 });
    
  });

  await t.step("large document handling", async () => {
    // Create a large document with nested arrays and objects
    const generateLargeDoc = (depth: number, breadth: number): Record<string, unknown> => {
      if (depth === 0) {
        return { value: "x".repeat(100) }; // Base case with a long string
      }
      
      const doc: Record<string, unknown> = {};
      // Add array of objects
      doc.array = Array(breadth).fill(null).map(() => generateLargeDoc(depth - 1, breadth));
      // Add nested objects
      doc.nested = generateLargeDoc(depth - 1, breadth);
      // Add some regular fields
      doc.number = Math.random() * 1000;
      doc.string = "test".repeat(10);
      doc.date = new Date();
      
      return doc;
    };

    // Generate a large document (~1MB)
    const largeDoc = generateLargeDoc(3, 5);
    
    // Should successfully insert large document
    const result = await collection.insertOne(largeDoc);
    assert(result.insertedId instanceof ObjectId);
    
    // Verify we can retrieve the large document
    const inserted = await collection.findOne({ _id: result.insertedId });
    assertEquals(inserted?.nested, largeDoc.nested);
    assertEquals(inserted?.array, largeDoc.array);
  });

});

Deno.test("Collection.insertMany", async (t) => {
  using kv = await Deno.openKv(":memory:");
  const collection = new Collection(kv, "test_collection");

  await t.step("multiple document insertion", async () => {
    const docs = [
      { name: "doc1", value: 1 },
      { name: "doc2", value: 2 },
      { name: "doc3", value: 3 },
    ];
    
    const result = await collection.insertMany(docs);
    assertEquals(result.insertedCount, 3);
    assertEquals(result.insertedIds.length, 3);
    
    // Verify all documents were inserted
    for (let i = 0; i < docs.length; i++) {
      const inserted = await collection.findOne({ _id: result.insertedIds[i] });
      assertEquals(inserted?.name, docs[i].name);
      assertEquals(inserted?.value, docs[i].value);
    }
  });

  await t.step("ordered vs unordered insertion", async () => {
    const customId = new ObjectId();
    const docs = [
      { name: "doc1", value: 1 },
      { _id: customId, name: "doc2", value: 2 }, // This will cause duplicate key error on second test
      { name: "doc3", value: 3 },
    ];

    // First insert the docs normally
    await collection.insertMany(docs);

    // Try inserting again with duplicate _id
    // Ordered insertion (default) should stop at error
    await assertRejects(
      async () => {
        await collection.insertMany(docs);
      },
      Error,
      "Duplicate key error"
    );

    // Unordered insertion should continue after error
    const result = await collection.insertMany(docs, { ordered: false });
    // Should have inserted docs 1 and 3, but failed on doc 2
    assertEquals(result.insertedCount, 2);
  });

  await t.step("empty array handling", async () => {
    const result = await collection.insertMany([]);
    assertEquals(result.insertedCount, 0);
    assertEquals(result.insertedIds.length, 0);
  });

  await t.step("large batch handling", async () => {
    // Create array of 1000 documents
    const largeBatch = Array.from({ length: 1000 }, (_, i) => ({
      index: i,
      name: `doc${i}`,
      data: "x".repeat(100), // Each doc has some size to it
    }));

    const result = await collection.insertMany(largeBatch);
    assertEquals(result.insertedCount, 1000);
    assertEquals(result.insertedIds.length, 1000);

    // Verify a few random documents
    const indices = [0, 499, 999];
    for (const idx of indices) {
      const doc = await collection.findOne({ _id: result.insertedIds[idx] });
      assertEquals(doc?.index, idx);
      assertEquals(doc?.name, `doc${idx}`);
    }
  });

  await t.step("error reporting accuracy", async () => {
    const docs = [
      { name: "doc1" },
      null as any, // Invalid document
      { name: "doc3" },
    ];

    // Should reject with specific error about invalid document
    await assertRejects(
      async () => {
        await collection.insertMany(docs);
      },
      Error,
      "Invalid document"
    );

    // With unordered insertion, should report error but continue
    const result = await collection.insertMany(docs, { ordered: false });
    assertEquals(result.insertedCount, 2); // Should insert docs 1 and 3
    
    // Check if errors were reported correctly
    assert(result.hasWriteErrors); // Use assert instead of assertTrue
    assertEquals(result.writeErrors?.length ?? 0, 1); // Handle potential undefined
  });
});

Deno.test("Collection.findOne", async (t) => {
  using kv = await Deno.openKv(":memory:");
  const collection = new Collection(kv, "test_collection");

  // Insert some test documents before each test
  const docs = [
    { _id: new ObjectId(), name: "John", age: 30, tags: ["a", "b"], nested: { x: 1 } },
    { _id: new ObjectId(), name: "Jane", age: 25, tags: ["b", "c"], nested: { x: 2 } },
    { _id: new ObjectId(), name: "Bob", age: 35, tags: ["a", "c"], nested: { x: 3 } },
  ];
  await collection.insertMany(docs);

  await t.step("basic document retrieval by _id", async () => {
    const result = await collection.findOne({ _id: docs[0]._id });
    assertEquals(result?._id, docs[0]._id);
    assertEquals(result?.name, docs[0].name);
  });

  await t.step("retrieval with simple field equality", async () => {
    const result = await collection.findOne({ name: "Jane" });
    assertEquals(result?.name, "Jane");
    assertEquals(result?.age, 25);
  });

  await t.step("comparison operators", async () => {
    // Test $eq
    const eqResult = await collection.findOne({ name: "John", age: { $eq: 30 } });
    assertEquals(eqResult?.name, "John");

    // Test $gt with debugging
    const gtResult = await collection.findOne({ name: "Bob", age: { $gt: 30 } });
    assertEquals(gtResult?.name, "Bob");
    assertEquals(gtResult?.name, "Bob");

    // Test $gte
    const gteResult = await collection.findOne({ name: "John", age: { $gte: 30 } });
    assertEquals(gteResult?.name, "John");

    // Test $lt
    const ltResult = await collection.findOne({ name: "Jane", age: { $lt: 30 } });
    assertEquals(ltResult?.name, "Jane");

    // Test $lte
    const lteResult = await collection.findOne({ name: "Jane", age: { $lte: 25 } });
    assertEquals(lteResult?.name, "Jane");

    // Test $ne
    const neResult = await collection.findOne({ name: "John", age: { $ne: 25 } });
    assertEquals(neResult?.name, "John");
  });

  await t.step("logical operators", async () => {
    // Test $and
    const andResult = await collection.findOne({
        $and: [
            { name: "John" },
            { age: { $gt: 25 } }, 
            { tags: "a" }
        ]
    });
    assertEquals(andResult?.name, "John");

    // Test $or
    const orResult = await collection.findOne({
        $or: [
            { name: "Jane" }, 
            { name: "Bob" }
        ]
    }, { sort: { name: 1 } }); // Explicitly sort by name
    assertEquals(orResult?.name, "Bob");
  });

  await t.step("array operators", async () => {
    // Test $all with more specific filter
    const allResult = await collection.findOne({
        name: "John",
        tags: { $all: ["a", "b"] }
    });
    assertEquals(allResult?.name, "John");

    // Test $size with more specific filter
    const sizeResult = await collection.findOne({
        name: "John",
        tags: { $size: 2 }
    });
    assertEquals(sizeResult?.name, "John");
  });

  await t.step("element operators", async () => {
    // Test $exists with more specific filter
    const existsResult = await collection.findOne({
        name: "John",  // Make it specific to John
        nested: { $exists: true }
    });
    assertEquals(existsResult?.name, "John");

    // Test non-existent field with specific document
    const notExistsResult = await collection.findOne({
        name: "John",  // Make it specific to John
        notAField: { $exists: false }
    });
    assertEquals(notExistsResult?.name, "John");

    // Add more specific test cases
    const existsFalseResult = await collection.findOne({
        name: "John",
        nonExistentField: { $exists: false }
    });
    assertEquals(existsFalseResult?.name, "John");

    // Test nested field exists
    const nestedExistsResult = await collection.findOne({
        name: "John",
        "nested.x": { $exists: true }
    });
    assertEquals(nestedExistsResult?.name, "John");
  });

  await t.step("projection tests", async () => {
    // Include specific fields
    const includeResult = await collection.findOne(
      { name: "John" },
      { projection: { name: 1, age: 1 } }
    );
    assertEquals(Object.keys(includeResult || {}).sort(), ["_id", "name", "age"].sort());

    // Exclude specific fields
    const excludeResult = await collection.findOne(
      { name: "John" },
      { projection: { tags: 0, nested: 0 } }
    );
    assertEquals(Object.keys(excludeResult || {}).sort(), ["_id", "name", "age"].sort());

    // Nested field projection
    interface DocWithNested {
      _id?: ObjectId;
      nested?: { x: number };
    }

    const nestedResult = await collection.findOne(
      { name: "John" },
      { projection: { "nested.x": 1 } }
    ) as DocWithNested | null;

    assertEquals(nestedResult?.nested?.x, 1);
    assertEquals(Object.keys(nestedResult || {}).length, 2); // _id and nested only

    // Test mixed inclusion/exclusion (should fail)
    await assertRejects(
      async () => {
        await collection.findOne(
          { name: "John" },
          { projection: { name: 1, age: 1, tags: 0 } }
        );
      },
      Error,
      "Projection cannot have a mix of inclusion and exclusion"
    );
  });

  await t.step("non-existent document handling", async () => {
    const result = await collection.findOne({ name: "NotExists" });
    assertEquals(result, null);
  });
});

Deno.test("Collection.find", async (t) => {
  using kv = await Deno.openKv(":memory:");

  const collection = new Collection<TestDoc>(kv, "test_collection");

  // Insert test documents
  const docs: TestDoc[] = [
    { _id: new ObjectId(), name: "John", age: 30, tags: ["a", "b"], nested: { x: 1 } },
    { _id: new ObjectId(), name: "Jane", age: 25, tags: ["b", "c"], nested: { x: 2 } },
    { _id: new ObjectId(), name: "Bob", age: 35, tags: ["a", "c"], nested: { x: 3 } },
    { _id: new ObjectId(), name: "Alice", age: 28, tags: ["b", "d"], nested: { x: 4 } },
    { _id: new ObjectId(), name: "Charlie", age: 32, tags: ["c", "d"], nested: { x: 5 } }
  ];
  await collection.insertMany(docs);

  await t.step("basic query with empty filter", async () => {
    const results = await collection.find({});
    assertEquals(results.length, docs.length);
  });

  await t.step("pagination (skip and limit)", async () => {
    const results = await collection.find({}, { skip: 2, limit: 2 });
    assertEquals(results.length, 2);
    // Ensure consistent ordering with sort
    const sortedResults = await collection.find({}, { sort: { name: 1 }, skip: 2, limit: 2 });
    assertEquals(sortedResults.map(d => d.name), ["Charlie", "Jane"]);
  });

  await t.step("sorting (single field and multiple fields)", async () => {
    // Single field sort
    const singleSort = await collection.find({}, { sort: { age: 1 } });
    assertEquals(singleSort.map(d => d.name), ["Jane", "Alice", "John", "Charlie", "Bob"]);

    // Multiple field sort
    const multiSort = await collection.find({}, { 
      sort: { age: -1, name: 1 } 
    });
    assertEquals(multiSort.map(d => d.name), ["Bob", "Charlie", "John", "Alice", "Jane"]);
  });

  await t.step("complex filters with multiple operators", async () => {
    const results = await collection.find({
      $and: [
        { age: { $gte: 30 } },
        { tags: { $all: ["c"] } }
      ]
    });
    assertEquals(results.length, 2);
    assert(results.every(doc => doc.age >= 30 && doc.tags.includes("c")));
  });

  await t.step("array field queries", async () => {
    const hasTag = await collection.find({ tags: "b" });
    assertEquals(hasTag.length, 3);
    assert(hasTag.every(doc => doc.tags.includes("b")));

    const exactTags = await collection.find({ tags: ["b", "c"] });
    assertEquals(exactTags.length, 1);
    assertEquals(exactTags[0].name, "Jane");
  });

  await t.step("nested document queries", async () => {
    const results = await collection.find({ "nested.x": { $gt: 3 } });
    assertEquals(results.length, 2);
    assert(results.every(doc => 
      (doc as { nested: { x: number } }).nested.x > 3
    ));
  });

  await t.step("projection combinations", async () => {
    const results = await collection.find(
      { age: { $gte: 30 } },
      { projection: { name: 1, age: 1 } }
    );

    assert(results.length > 0);
    results.forEach(doc => {
      assertEquals(Object.keys(doc).sort(), ["_id", "name", "age"].sort());
      assert(doc.age >= 30);
    });
  });

  await t.step("edge cases (empty results, max limit)", async () => {
    // Empty results
    const noMatches = await collection.find({ age: { $gt: 100 } });
    assertEquals(noMatches.length, 0);

    // Max limit (if implemented)
    const allDocs = await collection.find({}, { limit: 1000 });
    assertEquals(allDocs.length, docs.length);
  });
});

Deno.test("Collection.countDocuments", async (t) => {
  using kv = await Deno.openKv(":memory:");

  const collection = new Collection<TestDoc>(kv, "test_collection");

  // Insert test documents
  const docs: TestDoc[] = [
    { _id: new ObjectId(), name: "John", age: 30, tags: ["a", "b"], nested: { x: 1 } },
    { _id: new ObjectId(), name: "Jane", age: 25, tags: ["b", "c"], nested: { x: 2 } },
    { _id: new ObjectId(), name: "Bob", age: 35, tags: ["a", "c"], nested: { x: 3 } },
    { _id: new ObjectId(), name: "Alice", age: 28, tags: ["b", "d"], nested: { x: 4 } },
    { _id: new ObjectId(), name: "Charlie", age: 32, tags: ["c", "d"], nested: { x: 5 } }
  ];
  await collection.insertMany(docs);

  await t.step("count with empty filter", async () => {
    const count = await collection.countDocuments({});
    assertEquals(count, docs.length);
  });

  await t.step("count with simple equality filter", async () => {
    const count = await collection.countDocuments({ name: "John" });
    assertEquals(count, 1);
  });

  await t.step("count with complex query", async () => {
    const count = await collection.countDocuments({
      $and: [
        { age: { $gte: 30 } },
        { tags: { $all: ["c"] } }
      ]
    });
    assertEquals(count, 2);
  });

  await t.step("count with skip/limit", async () => {
    const count = await collection.countDocuments({}, { skip: 2, limit: 2 });
    assertEquals(count, 2);
  });

  await t.step("count on empty collection", async () => {
    const emptyCollection = new Collection<TestDoc>(kv, "empty_collection");
    const count = await emptyCollection.countDocuments({});
    assertEquals(count, 0);
  });

  await t.step("count with array operators", async () => {
    const count = await collection.countDocuments({ tags: { $size: 2 } });
    assertEquals(count, 5); // All docs have 2 tags

    const hasTagCount = await collection.countDocuments({ tags: "b" });
    assertEquals(hasTagCount, 3);
  });

  await t.step("count with logical operators", async () => {
    const count = await collection.countDocuments({
      $or: [
        { age: { $lt: 28 } },
        { age: { $gt: 32 } }
      ]
    });
    assertEquals(count, 2); // Jane (25) and Bob (35)
  });
});

Deno.test("Collection.estimatedDocumentCount", async (t) => {
  using kv = await Deno.openKv(":memory:");

  const collection = new Collection<TestDoc>(kv, "test_collection");

  await t.step("count on empty collection", async () => {
    const count = await collection.estimatedDocumentCount();
    assertEquals(count, 0);
  });

  await t.step("count on populated collection", async () => {
    // Insert test documents
    const docs: TestDoc[] = [
      { _id: new ObjectId(), name: "John", age: 30, tags: ["a", "b"], nested: { x: 1 } },
      { _id: new ObjectId(), name: "Jane", age: 25, tags: ["b", "c"], nested: { x: 2 } },
      { _id: new ObjectId(), name: "Bob", age: 35, tags: ["a", "c"], nested: { x: 3 } }
    ];
    await collection.insertMany(docs);

    const count = await collection.estimatedDocumentCount();
    assertEquals(count, docs.length);
  });

  await t.step("count accuracy verification", async () => {
    // Insert more documents and verify count increases
    const newDoc = { _id: new ObjectId(), name: "Alice", age: 28, tags: ["d"], nested: { x: 4 } };
    await collection.insertOne(newDoc);

    const updatedCount = await collection.estimatedDocumentCount();
    assertEquals(updatedCount, 4);

    // Compare with countDocuments
    const actualCount = await collection.countDocuments({});
    assertEquals(updatedCount, actualCount);
  });
});

Deno.test("Collection.distinct", async (t) => {
  using kv = await Deno.openKv(":memory:");

  const collection = new Collection<TestDoc>(kv, "test_collection");

  // Insert test documents
  const docs: TestDoc[] = [
    { _id: new ObjectId(), name: "John", age: 30, tags: ["a", "b"], nested: { x: 1, labels: ["active"] }, status: "active" },
    { _id: new ObjectId(), name: "Jane", age: 25, tags: ["b", "c"], nested: { x: 1, labels: ["pending"] }, status: null },
    { _id: new ObjectId(), name: "Bob", age: 30, tags: ["a", "c"], nested: { x: 2, labels: ["active"] }, status: "active" },
    { _id: new ObjectId(), name: "Alice", age: 25, tags: ["b", "d"], nested: { x: 2, labels: ["inactive"] }, status: "inactive" }
  ];
  await collection.insertMany(docs);

  await t.step("distinct on simple field", async () => {
    const ages = await collection.distinct("age");
    assertEquals(ages.sort(), [25, 30]);

    const names = await collection.distinct("name");
    assertEquals(names.sort(), ["Alice", "Bob", "Jane", "John"]);
  });

  await t.step("distinct on nested field", async () => {
    const nestedX = await collection.distinct("nested.x");
    assertEquals(nestedX.sort(), [1, 2]);
  });

  await t.step("distinct with filter", async () => {
    const agesActive = await collection.distinct("age", { status: "active" });
    assertEquals(agesActive, [30]);
  });

  await t.step("distinct on array field", async () => {
    const tags = await collection.distinct("tags");
    assertEquals(tags.sort(), ["a", "b", "c", "d"]);

    const nestedLabels = await collection.distinct("nested.labels");
    assertEquals(nestedLabels.sort(), ["active", "inactive", "pending"]);
  });

  await t.step("distinct with null/undefined values", async () => {
    const statuses = await collection.distinct("status");
    // Instead of comparing sorted arrays directly, let's verify the contents match
    assertEquals(new Set(statuses), new Set([null, "active", "inactive"]));
    // Or verify length and contents without caring about order
    assertEquals(statuses.length, 3);
    assert(statuses.includes(null));
    assert(statuses.includes("active"));
    assert(statuses.includes("inactive"));

    const nonExistentField = await collection.distinct("nonexistent");
    assertEquals(nonExistentField, []);
  });

  await t.step("distinct on non-existent field", async () => {
    const values = await collection.distinct("missing");
    assertEquals(values, []);
  });

  await t.step("distinct with complex filter", async () => {
    const names = await collection.distinct("name", {
      $or: [
        { age: { $gt: 25 } },
        { "nested.x": 2 }
      ]
    });
    assertEquals(names.sort(), ["Alice", "Bob", "John"]);
  });
});

Deno.test("Collection.updateOne", async (t) => {
  using kv = await Deno.openKv(":memory:");

  const collection = new Collection<TestDoc>(kv, "test_collection");

  // Insert a test document before each test
  const doc = {
    _id: new ObjectId(),
    name: "John",
    age: 30,
    tags: ["a", "b"],
    nested: { x: 1 }
  } as TestDoc;  // Assert as TestDoc to ensure type compatibility
  await collection.insertOne(doc);

  await t.step("basic field update ($set)", async () => {
    const result = await collection.updateOne(
      { _id: doc._id },
      { $set: { name: "Jane", age: 25 } }
    );
    assertEquals(result.matchedCount, 1);
    assertEquals(result.modifiedCount, 1);

    const updated = await collection.findOne({ _id: doc._id });
    assertEquals(updated?.name, "Jane");
    assertEquals(updated?.age, 25);
  });

  await t.step("numeric field operations", async () => {
    // Test $inc
    await collection.updateOne(
      { _id: doc._id },
      { $inc: { age: 5, counter: 1 } }
    );
    let updated = await collection.findOne({ _id: doc._id });
    assertEquals(updated?.age, 30); // 25 + 5
    assertEquals(updated?.counter, 1); // undefined + 1

    // Test $mul
    await collection.updateOne(
      { _id: doc._id },
      { $mul: { age: 2, counter: 3 } }
    );
    updated = await collection.findOne({ _id: doc._id });
    assertEquals(updated?.age, 60); // 30 * 2
    assertEquals(updated?.counter, 3); // 1 * 3
  });

  await t.step("array operations", async () => {
    // Test $push
    await collection.updateOne(
      { _id: doc._id },
      { $push: { tags: "c" } }
    );
    let updated = await collection.findOne({ _id: doc._id });
    assertEquals(updated?.tags, ["a", "b", "c"]);

    // Test $pull
    await collection.updateOne(
      { _id: doc._id },
      { $pull: { tags: "b" } }
    );
    updated = await collection.findOne({ _id: doc._id });
    assertEquals(updated?.tags, ["a", "c"]);

    // Test $addToSet
    await collection.updateOne(
      { _id: doc._id },
      { $addToSet: { tags: "c" } } // Should not add duplicate
    );
    updated = await collection.findOne({ _id: doc._id });
    assertEquals(updated?.tags, ["a", "c"]);
  });

  await t.step("nested field updates", async () => {
    await collection.updateOne(
      { _id: doc._id },
      { 
        $set: { "nested.y": 2 },
        $inc: { "nested.x": 1 }
      }
    );
    const updated = await collection.findOne({ _id: doc._id });
    assertEquals(updated?.nested, { x: 2, y: 2 });
  });

  await t.step("upsert behavior", async () => {
    const newId = new ObjectId();
    const result = await collection.updateOne(
      { _id: newId },
      { $set: { name: "Bob", age: 35 } },
      { upsert: true }
    );
    assertEquals(result.upsertedCount, 1);
    assertEquals(result.upsertedId, newId);

    const upserted = await collection.findOne({ _id: newId });
    assertEquals(upserted?.name, "Bob");
  });

  await t.step("multiple operator combination", async () => {
    const result = await collection.updateOne(
      { _id: doc._id },
      {
        $set: { status: "active" },
        $inc: { age: 1 },
        $push: { tags: "d" },
        $unset: { counter: true }
      }
    );
    assertEquals(result.modifiedCount, 1);

    const updated = await collection.findOne({ _id: doc._id });
    assertEquals(updated?.status, "active");
    assertEquals(updated?.age, 61);
    assert(updated?.tags.includes("d"));
    assertEquals(updated?.counter, undefined);
  });

  await t.step("update non-existent document", async () => {
    const result = await collection.updateOne(
      { _id: new ObjectId() },
      { $set: { name: "Missing" } }
    );
    assertEquals(result.matchedCount, 0);
    assertEquals(result.modifiedCount, 0);
  });

  await t.step("field removal ($unset)", async () => {
    await collection.updateOne(
      { _id: doc._id },
      { $unset: { status: true, "nested.y": true } }
    );
    const updated = await collection.findOne({ _id: doc._id });
    assertEquals(updated?.status, undefined);
    assertEquals((updated as { nested: { y: number | undefined } })?.nested.y, undefined);
  });

  await t.step("min/max operations", async () => {
    // Test $min
    await collection.updateOne(
      { _id: doc._id },
      { $min: { age: 50 } }
    );
    let updated = await collection.findOne({ _id: doc._id });
    assertEquals(updated?.age, 50); // 61 -> 50

    // Test $max
    await collection.updateOne(
      { _id: doc._id },
      { $max: { age: 55 } }
    );
    updated = await collection.findOne({ _id: doc._id });
    assertEquals(updated?.age, 55); // 50 -> 55
  });
});

Deno.test("Collection.updateMany", async (t) => {
  using kv = await Deno.openKv(":memory:");
  const collection = new Collection<TestDoc>(kv, "test_collection");

  // Helper to reset collection state
  const resetCollection = async () => {
    // Clear existing data
    const prefix = ["test_collection"];
    for await (const entry of kv.list({ prefix })) {
      await kv.delete(entry.key);
    }
    
    // Insert fresh test documents
    const docs = [
      { _id: new ObjectId(), name: "John", age: 30, status: "active", tags: ["a", "b"], nested: { x: 1 } },
      { _id: new ObjectId(), name: "Jane", age: 25, status: "active", tags: ["b", "c"], nested: { x: 2 } },
      { _id: new ObjectId(), name: "Bob", age: 35, status: "inactive", tags: ["a", "c"], nested: { x: 3 } },
      { _id: new ObjectId(), name: "Alice", age: 28, status: "active", tags: ["b", "d"], nested: { x: 4 } }
    ];
    await collection.insertMany(docs);
    return docs;
  };

  await t.step("update multiple documents", async () => {
    const docs = await resetCollection();
    const result = await collection.updateMany(
      { status: "active" },
      { $set: { status: "updated" } }
    );
    assertEquals(result.matchedCount, 3);
    assertEquals(result.modifiedCount, 3);

    const updated = await collection.find({ status: "updated" });
    assertEquals(updated.length, 3);
  });

  await t.step("array updates across documents", async () => {
    const docs = await resetCollection();
    const result = await collection.updateMany(
      { tags: "b" },
      { $push: { tags: "new" } }
    );
    assertEquals(result.matchedCount, 3);
    assertEquals(result.modifiedCount, 3);

    const updated = await collection.find({ tags: "new" });
    assertEquals(updated.length, 3);
    updated.forEach(doc => {
      assert(doc.tags.includes("new"));
    });
  });

  await t.step("complex filter matching", async () => {
    const result = await collection.updateMany(
      { $and: [{ age: { $gte: 25 } }, { tags: "b" }] },
      { $inc: { age: 1 }, $set: { "nested.x": 10 } }
    );
    assertEquals(result.matchedCount, 3);
    assertEquals(result.modifiedCount, 3);

    const updated = await collection.find({ "nested.x": 10 });
    assertEquals(updated.length, 3);
    updated.forEach(doc => {
      assert(doc.age > 25);
    });
  });

  await t.step("empty result handling", async () => {
    const result = await collection.updateMany(
      { status: "nonexistent" },
      { $set: { status: "updated" } }
    );
    assertEquals(result.matchedCount, 0);
    assertEquals(result.modifiedCount, 0);
  });

  await t.step("upsert with multiple matches", async () => {
    const result = await collection.updateMany(
      { status: "missing" },
      { $set: { status: "new", age: 20 } },
      { upsert: true }
    );
    assertEquals(result.matchedCount, 0);
    assertEquals(result.modifiedCount, 1);
    assertEquals(result.upsertedCount, 1);
    assert(result.upsertedId !== null);

    const inserted = await collection.findOne({ status: "new" });
    assertEquals(inserted?.age, 20);
  });

});

Deno.test("Collection.deleteOne", async (t) => {
  using kv = await Deno.openKv(":memory:");
  const collection = new Collection<TestDoc>(kv, "test_collection");

  // Helper to reset collection state
  const resetCollection = async () => {
    // Clear existing data
    const prefix = ["test_collection"];
    for await (const entry of kv.list({ prefix })) {
      await kv.delete(entry.key);
    }
    
    // Insert fresh test documents
    const docs = [
      { _id: new ObjectId(), name: "John", age: 30, status: "active", tags: ["a", "b"] },
      { _id: new ObjectId(), name: "Jane", age: 25, status: "active", tags: ["b", "c"] },
      { _id: new ObjectId(), name: "Bob", age: 35, status: "inactive", tags: ["a", "c"] }
    ];
    await collection.insertMany(docs);
    return docs;
  };

  await t.step("delete by _id", async () => {
    const docs = await resetCollection();
    const result = await collection.deleteOne({ _id: docs[0]._id });
    assertEquals(result.acknowledged, true);
    assertEquals(result.deletedCount, 1);

    const deleted = await collection.findOne({ _id: docs[0]._id });
    assertEquals(deleted, null);
  });

  await t.step("delete with complex filter", async () => {
    const docs = await resetCollection();
    const result = await collection.deleteOne({
      $and: [{ age: { $gt: 25 } }, { tags: "b" }]
    });
    assertEquals(result.deletedCount, 1);

    const remaining = await collection.find({ age: { $gt: 25 }, tags: "b" });
    assertEquals(remaining.length, 0);
  });

  await t.step("delete non-existent document", async () => {
    await resetCollection();
    const result = await collection.deleteOne({ name: "NonExistent" });
    assertEquals(result.deletedCount, 0);
  });

  await t.step("delete with empty filter", async () => {
    await resetCollection();
    const result = await collection.deleteOne({});
    assertEquals(result.deletedCount, 1);

    const remaining = await collection.countDocuments({});
    assertEquals(remaining, 2);
  });

  await t.step("atomic operation verification", async () => {
    const docs = await resetCollection();
    const doc = docs[0];

    // Try to delete with wrong versionstamp
    const key = ["test_collection", doc._id.id] as const;
    await assertRejects(
      async () => {
        await kv.atomic()
          .check({ key, versionstamp: "wrong" })
          .delete(key)
          .commit();
      },
      Error,
      "Invalid check"
    );

    // Document should still exist
    const stillExists = await collection.findOne({ _id: doc._id });
    assert(stillExists !== null);
  });
});

Deno.test("Collection.deleteMany", async (t) => {
  using kv = await Deno.openKv(":memory:");
  const collection = new Collection<TestDoc>(kv, "test_collection");

  // Helper to reset collection state
  const resetCollection = async () => {
    // Clear existing data
    const prefix = ["test_collection"];
    for await (const entry of kv.list({ prefix })) {
      await kv.delete(entry.key);
    }
    
    // Insert fresh test documents
    const docs = [
      { _id: new ObjectId(), name: "John", age: 30, status: "active", tags: ["a", "b"] },
      { _id: new ObjectId(), name: "Jane", age: 25, status: "active", tags: ["b", "c"] },
      { _id: new ObjectId(), name: "Bob", age: 35, status: "inactive", tags: ["a", "c"] },
      { _id: new ObjectId(), name: "Alice", age: 28, status: "active", tags: ["b", "d"] }
    ];
    await collection.insertMany(docs);
    return docs;
  };

  await t.step("delete multiple documents", async () => {
    const docs = await resetCollection();
    const result = await collection.deleteMany({ status: "active" });
    assertEquals(result.acknowledged, true);
    assertEquals(result.deletedCount, 3);

    const remaining = await collection.countDocuments({});
    assertEquals(remaining, 1);
  });

  await t.step("delete with complex filter", async () => {
    const docs = await resetCollection();
    const result = await collection.deleteMany({
      $and: [
        { age: { $gte: 25 } },
        { tags: "b" }
      ]
    });
    assertEquals(result.deletedCount, 3);

    const remaining = await collection.find({ tags: "b" });
    assertEquals(remaining.length, 0);
  });

  await t.step("delete all documents", async () => {
    await resetCollection();
    const result = await collection.deleteMany({});
    assertEquals(result.deletedCount, 4);

    const remaining = await collection.countDocuments({});
    assertEquals(remaining, 0);
  });

  await t.step("delete non-matching filter", async () => {
    await resetCollection();
    const result = await collection.deleteMany({ status: "nonexistent" });
    assertEquals(result.deletedCount, 0);

    const remaining = await collection.countDocuments({});
    assertEquals(remaining, 4);
  });

  await t.step("transaction atomicity", async () => {
    const docs = await resetCollection();
    
    // Try to delete with wrong versionstamp
    const key = ["test_collection", docs[0]._id.id] as const;
    await assertRejects(
      async () => {
        await kv.atomic()
          .check({ key, versionstamp: "wrong" })
          .delete(key)
          .commit();
      },
      Error,
      "Invalid check"
    );

    // All documents should still exist
    const remaining = await collection.countDocuments({});
    assertEquals(remaining, 4);
  });

  await t.step("delete count accuracy", async () => {
    const docs = await resetCollection();
    
    // Delete in multiple operations and verify counts
    const result1 = await collection.deleteMany({ status: "active" });
    assertEquals(result1.deletedCount, 3);
    
    const result2 = await collection.deleteMany({ status: "inactive" });
    assertEquals(result2.deletedCount, 1);
    
    const finalCount = await collection.countDocuments({});
    assertEquals(finalCount, 0);
  });
});

Deno.test("ObjectId", async (t) => {
  await t.step("generates unique ids", () => {
    const id1 = new ObjectId();
    const id2 = new ObjectId();
    assert(id1.toString() !== id2.toString());
    assertEquals(id1.toString().length, 24);
  });

  await t.step("creates from string", () => {
    const id = new ObjectId();
    const idString = id.toString();
    const reconstructed = new ObjectId(idString);
    assertEquals(id.toString(), reconstructed.toString());
  });

  await t.step("validates ids", () => {
    assert(ObjectId.isValid("507f1f77bcf86cd799439011"));
    assert(!ObjectId.isValid("invalid"));
    assert(!ObjectId.isValid("507f1f77bcf86cd79943901")); // Too short
    assert(!ObjectId.isValid("507f1f77bcf86cd7994390111")); // Too long
  });

  await t.step("extracts timestamp", () => {
    const now = new Date();
    const id = new ObjectId();
    const timestamp = id.getTimestamp();
    assert(timestamp.getTime() >= now.getTime() - 1000); // Within 1 second
    assert(timestamp.getTime() <= now.getTime() + 1000);
  });

  await t.step("creates from timestamp", () => {
    const date = new Date("2023-01-01");
    const id = ObjectId.createFromTime(date.getTime() / 1000); // Convert to Unix timestamp in seconds
    assertEquals(id.getTimestamp().getTime(), date.getTime());
  });

  await t.step("compares ids", () => {
    const id1 = new ObjectId();
    const id2 = new ObjectId(id1.toString());
    assert(id1.equals(id2));
  });
});

Deno.test("Collection.createIndex", async (t) => {
  const collectionName = `test_collection_${crypto.randomUUID()}`;
  using kv = await Deno.openKv(":memory:");
  const collection = new Collection<TestDoc>(kv, collectionName);

  // Add index cleanup between steps
  const clearIndexes = async () => {
    const indexes = await collection.listIndexes();
    for (const index of indexes) {
      if (index.name !== "_id_") { // Don't remove the default _id index
        await collection.dropIndex(index.name);
      }
    }
  };

  await t.step("index options validation", async () => {
    await clearIndexes();
    // Invalid options should fail
    await assertRejects(
      async () => {
        await collection.createIndex("test", {
          invalid: true,
          another: false
        } as any);
      },
      Error,
      "Invalid index options"
    );

    // Valid options should work
    const validResult = await collection.createIndex("test", { 
      unique: true,
      sparse: true,
      name: "custom_name"
    });
    assertEquals(validResult, "custom_name");
  });

  await t.step("basic index operations", async () => {
    await clearIndexes();
    const ascIndexName = await collection.createIndex("age");
    assertEquals(ascIndexName, "age_1");
  });

  await t.step("unique index enforcement", async () => {
    await clearIndexes();
    // Create unique index on email
    await collection.createIndex("email", { unique: true });
    
    // Insert first document
    await collection.insertOne({
      name: "John",
      email: "john@test.com",
      age: 30,
      tags: ["a"],
      nested: { value: 1 }
    });

    // Try to insert duplicate email - should fail
    await assertRejects(
      async () => {
        await collection.insertOne({
          name: "Jane",
          email: "john@test.com", // Duplicate
          age: 25,
          tags: ["b"],
          nested: { value: 2 }
        });
      },
      Error,
      "Duplicate key error"
    );
  });
});

Deno.test("Collection.find with indexes", async (t) => {
  const collectionName = `test_collection_${crypto.randomUUID()}`;
  using kv = await Deno.openKv(":memory:");
  const collection = new Collection<TestDoc>(kv, collectionName);

  // Add index cleanup to existing data cleanup
  const clearAll = async () => {
    // Clear documents
    const docs = await collection.find({});
    for (const doc of docs) {
      await collection.deleteOne({ _id: doc._id });
    }
    // Clear indexes
    const indexes = await collection.listIndexes();
    for (const index of indexes) {
      if (index.name !== "_id_") {
        await collection.dropIndex(index.name);
      }
    }
  };

  const setupTestData = async () => {
    await clearAll(); // Changed from clearData to clearAll
    await collection.insertMany([
      { name: "John", age: 30, email: "john@test.com", status: "active" },
      { name: "Jane", age: 25, email: "jane@test.com", status: "active" },
      { name: "Bob", age: 35, email: "bob@test.com", status: "inactive" },
      { name: "Alice", age: 28, email: "alice@test.com", status: "active" }
    ]);
  };

  await t.step("exact match with single field index", async () => {
    await setupTestData();
    await collection.createIndex("status");

    const docs = await collection.find({ status: "active" });
    assertEquals(docs.length, 3);
    assert(docs.every(doc => doc.status === "active"));
  });

  await t.step("range query with index", async () => {
    await setupTestData();
        await collection.createIndex("age");

    const docs = await collection.find({ 
      age: { $gte: 25, $lt: 35 } 
    });
    assertEquals(docs.length, 3);
    assert(docs.every(doc => 
      (doc as { age: number }).age >= 25 && 
      (doc as { age: number }).age < 35
    ));
  });

  await t.step("compound index query", async () => {
    await setupTestData();
    await collection.createIndex({ 
      key: { status: 1, age: 1 } 
    });

    const docs = await collection.find({ 
      status: "active",
      age: { $gte: 25 }
    });
    assertEquals(docs.length, 3);
    assert(docs.every(doc => 
      doc.status === "active" && 
      (doc as { age: number }).age >= 25
    ));
  });

  await t.step("index with sort", async () => {
    await setupTestData();
    await collection.createIndex("age");

    const docs = await collection.find(
      { age: { $gte: 25 } },
      { sort: { age: -1 } }
    );
    assertEquals(docs.length, 4);
    assert((docs[0] as { age: number }).age > 
           (docs[1] as { age: number }).age); // Verify descending order
  });

  await t.step("index with projection", async () => {
    await setupTestData();
    await collection.createIndex("status");

    const docs = await collection.find(
      { status: "active" },
      { projection: { name: 1, email: 1 } }
    );
    assertEquals(docs.length, 3);
    assert(docs.every(doc => 
      Object.keys(doc).length === 3 && // _id, name, email
      doc.name !== undefined &&
      doc.email !== undefined &&
      doc.status === undefined
    ));
  });

  await t.step("date range query with index", async () => {
    await clearAll();
    
    // Create dates for testing
    const now = new Date();
    const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const tomorrow = new Date(now.getTime() + 24 * 60 * 60 * 1000);
    
    // Insert test documents with dates
    await collection.insertMany([
      { name: "Doc1", createdAt: yesterday, status: "active" },
      { name: "Doc2", createdAt: now, status: "active" },
      { name: "Doc3", createdAt: tomorrow, status: "active" },
    ]);

    // Create index on date field
    await collection.createIndex("createdAt");

    // Test range query
    const docs = await collection.find({ 
      createdAt: { 
        $gte: yesterday,
        $lt: tomorrow 
      }
    });

    assertEquals(docs.length, 2);
    assert(docs.every(doc => 
      (doc as { createdAt: Date }).createdAt >= yesterday && 
      (doc as { createdAt: Date }).createdAt < tomorrow
    ));

    // Test sorting by date
    const sortedDocs = await collection.find(
      { createdAt: { $gte: yesterday } },
      { sort: { createdAt: -1 } }
    );

    assertEquals(sortedDocs.length, 3);
    assert((sortedDocs[0] as { createdAt: Date }).createdAt > 
           (sortedDocs[1] as { createdAt: Date }).createdAt);
  });
}); 