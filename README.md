# Dengo

<div align="center">
  <img src="logo.png" alt="Dengo Logo" width="200"/>
  <h3>MongoDB-compatible API for Deno KV</h3>
  <p>The power of MongoDB queries with the simplicity of Deno's native KV store</p>

[![JSR](https://jsr.io/badges/@copilotz/dengo)](https://jsr.io/@copilotz/dengo)
[![GitHub stars](https://img.shields.io/github/stars/copilotzhq/dengo)](https://github.com/copilotzhq/dengo)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

</div>

## ✨ Features

- **MongoDB API**: Use the familiar MongoDB query language you already know
- **Native Deno KV**: Built on Deno's built-in key-value store with no external
  dependencies
- **Type Safety**: First-class TypeScript support with generics for collections
- **Indexing**: Create indexes for faster queries and unique constraints
- **Serverless Ready**: Perfect for Deno Deploy and edge functions
- **Comprehensive**: Support for find, update, delete, and aggregation
  operations

## 📦 Installation

```bash
# Import from JSR
import { Database, Document, ObjectId } from "@copilotz/dengo";

# Or import directly from GitHub
import { Database, Document, ObjectId } from "https://raw.githubusercontent.com/copilotzhq/dengo/main/mod.ts";
```

## 🚀 Quick Start

```typescript
// Initialize the database with Deno KV
const db = new Database(await Deno.openKv());

// Define your document type (optional but recommended)
interface User extends Document {
  name: string;
  email: string;
  age: number;
  tags: string[];
  createdAt: Date;
}

// Get a typed collection
const users = db.collection<User>("users");

// Create an index for faster queries
await users.createIndex({ key: { email: 1 }, options: { unique: true } });

// Insert a document
const result = await users.insertOne({
  name: "John Doe",
  email: "john@example.com",
  age: 30,
  tags: ["developer", "deno"],
  createdAt: new Date(),
});

// Find documents with MongoDB query syntax
const youngDevelopers = await users.find({
  age: { $lt: 35 },
  tags: "developer",
}).sort({ createdAt: -1 }).limit(10);

// Update documents
await users.updateMany(
  { tags: "developer" },
  { $set: { verified: true }, $push: { tags: "verified" } },
);

// Delete documents
await users.deleteOne({ email: "john@example.com" });
```

## 📋 Supported MongoDB Features

Dengo implements a wide range of MongoDB features:

### Query Operators

- **Comparison**: `$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$ne`, `$in`, `$nin`
- **Logical**: `$and`, `$or`, `$not`, `$nor`
- **Element**: `$exists`, `$type`
- **Array**: `$all`, `$elemMatch`, `$size`

### Update Operators

- **Fields**: `$set`, `$unset`, `$inc`, `$mul`, `$min`, `$max`, `$rename`
- **Arrays**: `$push`, `$pull`, `$pullAll`, `$pop`, `$addToSet`

### Collection Methods

- **CRUD Operations**:
  - `insertOne`, `insertMany`
  - `findOne`, `find`
  - `updateOne`, `updateMany`
  - `deleteOne`, `deleteMany`
  - `countDocuments`, `estimatedDocumentCount`
  - `distinct`

- **Index Management**:
  - `createIndex`
  - `dropIndex`
  - `listIndexes`

### Query Options

- **Find Options**:
  - `projection` - Include or exclude fields
  - `sort` - Sort results by specified fields
  - `skip` - Skip a specified number of documents
  - `limit` - Limit the number of returned documents

- **Update Options**:
  - `upsert` - Insert document if no match is found

- **Insert Options**:
  - `ordered` - Control whether to continue on error

### Indexes

- Single field indexes
- Compound indexes
- Unique constraints
- Sparse indexes

See our [MongoDB Compatibility](./MONGODB_COMPAT.md) document for a detailed
comparison.

## 🔍 Examples

### Todo Application

```typescript
// Define the Todo type
interface Todo {
  _id: ObjectId;
  title: string;
  completed: boolean;
  dueDate?: Date;
  tags: string[];
  priority: "low" | "medium" | "high";
  userId: string;
}

// Initialize collection
const todos = db.collection<Todo>("todos");

// Create indexes for common queries
await todos.createIndex({ key: { userId: 1 } });
await todos.createIndex({ key: { userId: 1, completed: 1 } });
await todos.createIndex({ key: { dueDate: 1 } });

// Add a new todo
await todos.insertOne({
  title: "Complete Dengo documentation",
  completed: false,
  dueDate: new Date("2023-12-31"),
  tags: ["work", "documentation"],
  priority: "high",
  userId: "user123",
});

// Find incomplete high-priority todos
const highPriorityTodos = await todos.find({
  userId: "user123",
  completed: false,
  priority: "high",
}).sort({ dueDate: 1 });

// Mark a todo as complete
await todos.updateOne(
  { _id: new ObjectId("...") },
  { $set: { completed: true } },
);

// Find todos by tag
const workTodos = await todos.find({
  userId: "user123",
  tags: "work",
});

// Delete completed todos
await todos.deleteMany({
  userId: "user123",
  completed: true,
});
```

Check out our [examples directory](./examples) for more complete examples.

## 📊 Performance

TO DO: Compare between Dengo and MongoDB

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

See [CONTRIBUTING.md](./CONTRIBUTING.md) for more information.

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file
for details.

## ⭐ Show Your Support

If you find Dengo useful, please consider giving it a star on GitHub! It helps
the project grow and improve.

[![GitHub stars](https://img.shields.io/github/stars/copilotzhq/dengo?style=social)](https://github.com/copilotzhq/dengo)

## 🙏 Acknowledgements

- The Deno team for creating an amazing runtime
- MongoDB for their excellent query API design
- All our contributors and testers and future users

---

<div align="center">
  <sub>Built with ❤️ by the Copilotz team</sub>
</div>
