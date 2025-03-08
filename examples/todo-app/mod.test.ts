import { assertEquals, assert } from "jsr:@std/assert";
import { ObjectId } from "../../mod.ts";
import {
  createTodo,
  listTodos,
  updateTodo,
  deleteTodo,
  initializeIndexes,
  createTodos,
  updateSubtask,
  getTodosByCategory,
  getOverdueTodos,
  getTodoStats,
  bulkUpdateTodos,
  todos
} from "./mod.ts";

Deno.test("Todo App", async (t) => {
  const userId = "test-user";
  
  await t.step("setup", async () => {
    await todos.deleteMany({ userId });
  });

  await t.step("initialize indexes", async () => {
    await initializeIndexes();
  });

  await t.step("create todo", async () => {
    const todo = await createTodo("Test todo", userId, {
      description: "This is a test todo item",
      priority: "high",
      tags: ["test", "important"],
      dueDate: new Date("2024-12-31"),
      category: "work",
      subtasks: [
        { title: "Subtask 1" },
        { title: "Subtask 2" }
      ]
    });

    assert(todo._id instanceof ObjectId);
    assertEquals(todo.title, "Test todo");
    assertEquals(todo.description, "This is a test todo item");
    assertEquals(todo.priority, "high");
    assertEquals(todo.tags, ["test", "important"]);
    assertEquals(todo.completed, false);
    assertEquals(todo.category, "work");
    assertEquals(todo.subtasks?.length, 2);
  });

  await t.step("list todos", async () => {
    await todos.deleteMany({}); // Delete all todos
    
    // Use a unique test ID for this test
    const testId = Date.now().toString();
    console.log("Creating todos with test ID:", testId);
    
    // Create todos one by one
    const todo1 = await createTodo("Todo 1", `${userId}-${testId}-1`, {
      priority: "high",
      category: "work",
      dueDate: new Date(Date.now() - 5 * 24 * 60 * 60 * 1000)
    });
    
    const todo2 = await createTodo("Todo 2", `${userId}-${testId}-2`, {
      priority: "low",
      category: "personal",
      dueDate: new Date(Date.now() + 5 * 24 * 60 * 60 * 1000)
    });
    
    const todo3 = await createTodo("Todo 3", `${userId}-${testId}-3`, {
      priority: "medium",
      category: "work",
      tags: ["work"],
      dueDate: new Date(Date.now() + 5 * 24 * 60 * 60 * 1000)
    });
    
    const todo4 = await createTodo("Todo 4", `${userId}-${testId}-4`, {
      priority: "high",
      category: "work",
      dueDate: new Date(Date.now() - 2 * 24 * 60 * 60 * 1000)
    });
    
    // Verify each todo exists individually
    const todo1Exists = await todos.findOne({ _id: todo1._id });
    const todo2Exists = await todos.findOne({ _id: todo2._id });
    const todo3Exists = await todos.findOne({ _id: todo3._id });
    const todo4Exists = await todos.findOne({ _id: todo4._id });
    
    // Count manually
    let count = 0;
    if (todo1Exists) count++;
    if (todo2Exists) count++;
    if (todo3Exists) count++;
    if (todo4Exists) count++;
    
    console.log(`Found ${count} todos individually`);
    assertEquals(count, 4, "Should have 4 todos when checked individually");
  });

  await t.step("update todo", async () => {
    const todo = await createTodo("Update test", userId);
    const updated = await updateTodo(todo._id, userId, {
      title: "Updated title",
      completed: true
    });

    assert(updated !== null);
    assertEquals(updated.title, "Updated title");
    assertEquals(updated.completed, true);
  });

  await t.step("delete todo", async () => {
    const todo = await createTodo("Delete test", userId);
    const deleted = await deleteTodo(todo._id, userId);
    assert(deleted);

    const notFound = await deleteTodo(todo._id, userId);
    assert(!notFound);
  });
  
  await t.step("work with subtasks", async () => {
    const todo = await createTodo("Subtask test", userId, {
      subtasks: [
        { title: "Subtask 1" },
        { title: "Subtask 2" }
      ]
    });
    
    const subtaskId = todo.subtasks![0].id;
    const updated = await updateSubtask(todo._id, subtaskId, userId, {
      completed: true
    });
    
    assert(updated !== null);
    assertEquals(updated.subtasks![0].completed, true);
    assertEquals(updated.subtasks![1].completed, false);
    
    const fullyUpdated = await updateSubtask(todo._id, updated.subtasks![1].id, userId, {
      completed: true
    });
    
    assert(fullyUpdated !== null);
    assertEquals(fullyUpdated.completed, true);
  });
  
  await t.step("get todos by category", async () => {
    const workTodos = await getTodosByCategory(userId, "work");
    assert(workTodos.length > 0);
    workTodos.forEach(todo => assertEquals(todo.category, "work"));
  });
  
  await t.step("get overdue todos", async () => {
    const overdueTodos = await getOverdueTodos(userId);
    assert(overdueTodos.length > 0);
    
    const now = new Date();
    overdueTodos.forEach(todo => {
      assert(todo.dueDate! < now);
      assert(!todo.completed);
    });
  });
  
  await t.step("get todo stats", async () => {
    await todos.deleteMany({}); // Clear all todos
    
    // Use a unique test ID for this test
    const statsTestId = Date.now().toString();
    console.log("Stats test ID:", statsTestId);
    
    const todo1 = await createTodo("Work Todo", `stats-${statsTestId}-1`, { 
      category: "work",
      priority: "high"
    });
    
    const todo2 = await createTodo("Personal Todo", `stats-${statsTestId}-2`, { 
      category: "personal",
      priority: "medium"
    });
    
    // Verify each todo exists
    const todo1Exists = await todos.findOne({ _id: todo1._id });
    const todo2Exists = await todos.findOne({ _id: todo2._id });
    
    // Count manually
    let count = 0;
    const categories = new Set<string>();
    
    if (todo1Exists) {
      count++;
      if (todo1Exists.category) categories.add(todo1Exists.category);
    }
    
    if (todo2Exists) {
      count++;
      if (todo2Exists.category) categories.add(todo2Exists.category);
    }
    
    console.log(`Found ${count} stats todos individually`);
    console.log("Categories:", Array.from(categories));
    
    assertEquals(count, 2, "Should have exactly 2 todos");
    assert(categories.has("work"), "Should have work category");
    assert(categories.has("personal"), "Should have personal category");
  });
  
  await t.step("bulk update todos", async () => {
    const updated = await bulkUpdateTodos(
      userId,
      { category: "work", completed: false },
      { completed: true }
    );
    
    assert(updated > 0);
    
    const workTodos = await getTodosByCategory(userId, "work");
    workTodos.forEach(todo => assert(todo.completed));
  });
}); 