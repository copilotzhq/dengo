# Dengo Todo App Example

This example demonstrates how to build a Todo application using Dengo, showcasing:
- Type-safe CRUD operations
- Indexing for performance
- Complex querying
- Real-world data modeling

## Features

- Create, read, update, and delete todos
- Mark todos as complete
- Add due dates and priority levels
- Tag-based organization
- User-specific todos
- Sorted by due date and priority

## Usage

```typescript
import { createTodo, listTodos, updateTodo, deleteTodo } from "./mod.ts";

// Create a new todo
const todo = await createTodo("Complete project", "user123", {
  priority: "high",
  tags: ["work", "urgent"],
  dueDate: new Date("2024-12-31")
});

// List todos
const todos = await listTodos("user123", {
  completed: false,
  priority: "high"
});

// Update a todo
const updated = await updateTodo(todo._id, "user123", {
  completed: true
});

// Delete a todo
const deleted = await deleteTodo(todo._id, "user123");
```

## Running Tests

```bash
deno test examples/todo-app/test.ts
```

## Key Concepts Demonstrated

1. **Type Safety**
   - Strongly typed Todo interface
   - Type-safe operations

2. **Indexing**
   - Compound indexes for efficient queries
   - User-specific indexing

3. **Query Operations**
   - Filter by multiple criteria
   - Sort by due date and priority

4. **Data Modeling**
   - Structured todo items
   - User ownership
   - Timestamps and metadata

5. **Best Practices**
   - Error handling
   - Data validation
   - Clean API design 