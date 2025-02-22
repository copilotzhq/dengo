# Contributing to Dengo

Thank you for your interest in contributing to Dengo! We aim to provide a MongoDB-like experience for Deno KV while maintaining type safety and zero dependencies.

## Getting Started

1. Fork and clone the repository
2. Create a new branch for your feature/fix
3. Install Deno if you haven't already (https://deno.land/manual/getting_started/installation)

## Development Guidelines

### Type Safety

We prioritize type safety throughout the codebase. Look at existing type definitions in `kv.ts`:

```typescript
interface FindOptions<T = Document> {
  sort?: Record<string, SortDirection>;
  limit?: number;
  skip?: number;
  projection?: Record<string, 0 | 1 | boolean>;
}

type Filter<T = any> = {
  [P in keyof T & string]?:
  | T[P]
  | ComparisonOperator<T[P]>
  | ArrayOperator<T[P]>
  | ElementOperator;
} & LogicalOperator<T>;
```

### Testing

All new features should include tests. Look at existing tests in `kv.test.ts` for examples:

```typescript
Deno.test("Collection.find", async (t) => {
  await t.step("basic query with empty filter", async () => {
    const results = await collection.find({});
    assertEquals(results.length, docs.length);
  });

  await t.step("pagination (skip and limit)", async () => {
    const results = await collection.find({}, { skip: 2, limit: 2 });
    assertEquals(results.length, 2);
  });
});
```

Key testing principles:
- Use descriptive test names
- Test edge cases
- Group related tests using `t.step()`
- Clean up test data after each test

### Code Style

- Use TypeScript features appropriately
- Follow existing patterns in the codebase
- Keep methods focused and well-named
- Add comments for complex logic
- Use private methods for internal functionality

### MongoDB Compatibility

When implementing features:
1. Match MongoDB's behavior where practical
2. Document any differences in behavior
3. Consider Deno KV's limitations
4. Optimize for edge computing use cases

## Pull Request Process

1. Ensure your code is formatted:
   ```bash
   deno fmt
   ```

2. Run tests:
   ```bash
   deno test
   ```

3. Include in your PR:
   - Description of changes
   - Test cases
   - Any breaking changes
   - Performance implications

## Running Examples

```typescript
// Basic usage
import { Database } from "./mod.ts";
const kv = await Deno.openKv();
const db = new Database(kv);

// Create a typed collection
interface User {
  name: string;
  email: string;
  age: number;
}

const users = db.collection<User>("users");

// Test your changes
await users.insertOne({
  name: "Test User",
  email: "test@example.com",
  age: 25
});
```

## Getting Help

- Open an issue for bugs or feature requests
- Ask questions in discussions
- Join our Discord community

## License

By contributing, you agree that your contributions will be licensed under the MIT License. 