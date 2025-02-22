# Dengo 🦕

A zero-dependency, type-safe MongoDB-like API for Deno KV with built-in indexing support.

[![JSR Score](https://jsr.io/badges/score/dengo)](https://jsr.io/dengo)
[![JSR Version](https://jsr.io/badges/version/dengo)](https://jsr.io/dengo)
[![Discord](https://img.shields.io/discord/123456789?label=Discord&logo=discord)](https://discord.gg/dengo)

## Why Dengo?

- 🚀 **MongoDB-like API**: Familiar syntax for MongoDB developers
- 💪 **100% Type-safe**: Full TypeScript support with generics
- 📇 **Smart Indexing**: Built-in support for single, compound, and unique indexes
- 🔍 **Rich Querying**: Complex queries, sorting, and pagination
- 🪶 **Zero Dependencies**: Built directly on Deno KV
- 🌐 **Edge-Ready**: Perfect for Deno Deploy
- ⚡ **High Performance**: Optimized for Deno KV's architecture

## Perfect for:

- Deno Deploy applications needing MongoDB-like querying
- Edge computing projects requiring robust data handling
- Jamstack applications with complex data requirements
- Serverless architectures demanding type safety

## Quick Start

```ts
import { Database } from "jsr:@dengo/dengo";

// Open KV store
const kv = await Deno.openKv();
const db = new Database(kv);

// Create a typed collection
interface User {
    name: string;
    email: string;
    age: number;
}

const users = db.collection<User>("users");

// Create an index for better query performance
await users.createIndex("email", { unique: true });

// Insert with type safety
await users.insertOne({
    name: "John Doe",
    email: "john@example.com",
    age: 30
});

// Rich querying with MongoDB syntax
const user = await users.findOne({
    age: { $gte: 25 },
    email: { $regex: "@example.com" }
});
```

## Advanced Features

### Powerful Indexing

```ts
// Single field index
await users.createIndex("email");

// Compound index
await users.createIndex({
    key: { status: 1, createdAt: -1 }
});

// Unique constraints
await users.createIndex("username", {
    unique: true,
    sparse: true
});
```

### Complex Queries

```ts
// Advanced filtering
const results = await users.find({
    age: { $gte: 18, $lte: 65 },
    status: "active",
    $or: [
        { role: "admin" },
        { permissions: { $in: ["moderate", "publish"] } }
    ]
});

// Sorting and pagination
const paginatedUsers = await users.find(
    { status: "active" },
    {
        sort: { lastLogin: -1 },
        skip: 20,
        limit: 10
    }
);
```

### Type-Safe Updates

```ts
// Atomic updates
await users.updateOne(
    { email: "john@example.com" },
    {
        $set: { status: "verified" },
        $inc: { loginCount: 1 },
        $push: { loginHistory: new Date() }
    }
);

// Bulk operations
await users.updateMany(
    { status: "pending" },
    {
        $set: { reviewed: true },
        $currentDate: { lastModified: true }
    }
);
```

## Performance

- Optimized index usage for faster queries
- Smart query planning
- Efficient bulk operations
- Minimal memory footprint

## Documentation & Support

- 📚 [Full Documentation](https://dengo.deno.dev)
- 💬 [Discord Community](https://discord.gg/dengo)
- 🐛 [Issue Tracker](https://github.com/dengo/dengo/issues)
- 📖 [API Reference](https://dengo.deno.dev/api)
- 🎓 [Tutorials](https://dengo.deno.dev/tutorials)

## Contributing

We welcome contributions! Check our [Contributing Guide](CONTRIBUTING.md) for details.

## License

MIT License - see [LICENSE](LICENSE) for details.