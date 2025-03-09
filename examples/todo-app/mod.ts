import { Database, Document, ObjectId } from "../../mod.ts";

// Types
interface Todo extends Document {
  title: string;
  completed: boolean;
  createdAt: Date;
  updatedAt: Date;
  dueDate?: Date;
  tags: string[];
  priority: "low" | "medium" | "high";
  userId: string;
  description?: string;
  subtasks?: {
    id: string;
    title: string;
    completed: boolean;
  }[];
  category?: string;
  assignedTo?: string;
}

// Database setup
const kv = await Deno.openKv();
const db = new Database(kv);
export const todos = db.collection<Todo>("todos");

// Initialize indexes function
export async function initializeIndexes() {
  try {
    // Check if we're creating a unique index on userId
    await todos.createIndex("userId", { unique: false }); // Make sure it's not unique
    
    await todos.createIndex({ 
      key: { userId: 1, completed: 1 } 
    });
    
    // Compound indexes for sorting and filtering
    await todos.createIndex({ 
      key: { dueDate: 1, priority: 1 } 
    });
    
    // Index for category-based queries
    await todos.createIndex({
      key: { userId: 1, category: 1 }
    });
    
    // Index for assignment-based queries
    await todos.createIndex("assignedTo");
    
  } catch (error) {
    console.error("Index creation error:", error);
    if (!(error instanceof Error) || !error.message.includes("Index already exists")) {
      throw error;
    }
  }
}

export async function createTodos(
  todosList: Omit<Todo, '_id'>[]
) {
  await todos.insertMany(todosList);
}

// Todo operations
export async function createTodo(
  title: string,
  userId: string,
  options: {
    description?: string;
    dueDate?: Date;
    tags?: string[];
    priority?: Todo["priority"];
    category?: string;
    subtasks?: { title: string; completed?: boolean }[];
    assignedTo?: string;
  } = {}
): Promise<Todo> {
  const now = new Date();
  
  // Process subtasks if provided
  const subtasks = options.subtasks?.map(task => ({
    id: crypto.randomUUID(),
    title: task.title,
    completed: task.completed || false
  }));
  
  const todoData = {
    title,
    completed: false,
    createdAt: now,
    updatedAt: now,
    dueDate: options.dueDate,
    tags: options.tags || [],
    priority: options.priority || "medium",
    userId,
    description: options.description,
    category: options.category,
    subtasks,
    assignedTo: options.assignedTo || userId,
  }

  const result = await todos.insertOne(todoData);
  const doc = todoData;

  return {
    ...doc,
    _id: result.insertedId,
  };
}

export async function listTodos(
  userId: string,
  options: {
    completed?: boolean;
    priority?: Todo["priority"];
    tag?: string;
    dueBefore?: Date;
    dueAfter?: Date;
    category?: string;
    assignedTo?: string;
    search?: string;
  } = {}
): Promise<Todo[]> {
  const filter: Record<string, unknown> = { userId };
  
  if (options.completed !== undefined) {
    filter.completed = options.completed;
  }
  
  if (options.priority) {
    filter.priority = options.priority;
  }
  
  if (options.tag) {
    filter.tags = { $in: [options.tag] };
  }
  
  if (options.dueBefore) {
    filter.dueDate = { ...(filter.dueDate || {}), $lte: options.dueBefore };
  }
  
  if (options.dueAfter) {
    filter.dueDate = { ...(filter.dueDate || {}), $gte: options.dueAfter };
  }
  
  if (options.category) {
    filter.category = options.category;
  }
  
  if (options.assignedTo) {
    filter.assignedTo = options.assignedTo;
  }
  
  // Simple text search implementation
  if (options.search) {
    const searchRegex = new RegExp(options.search, 'i');
    filter.$or = [
      { title: { $regex: searchRegex } },
      { description: { $regex: searchRegex } }
    ];
  }

  const cursor = await todos.find(filter, { 
    sort: { dueDate: 1, priority: -1 } 
  });

  return cursor.toArray();
}

export async function updateTodo(
  todoId: ObjectId,
  userId: string,
  updates: Partial<Omit<Todo, "_id" | "userId" | "createdAt">>
): Promise<Todo | null> {
  const updateDoc: Partial<Todo> = {
    ...updates,
    updatedAt: new Date(),
  };

  const result = await todos.updateOne(
    { _id: todoId, userId },
    { $set: updateDoc }
  );

  if (result.modifiedCount === 0) {
    return null;
  }

  const doc = await todos.findOne({ _id: todoId, userId });
  return doc;
}

export async function deleteTodo(
  todoId: ObjectId,
  userId: string
): Promise<boolean> {
  const result = await todos.deleteOne({ _id: todoId, userId });
  return result.deletedCount > 0;
}

// New functions to showcase more capabilities

export async function updateSubtask(
  todoId: ObjectId,
  subtaskId: string,
  userId: string,
  updates: { title?: string; completed?: boolean }
): Promise<Todo | null> {
  // First find the todo to get the subtasks
  const todo = await todos.findOne({ _id: todoId, userId });
  if (!todo || !todo.subtasks) return null;
  
  // Find and update the specific subtask
  const updatedSubtasks = todo.subtasks.map(subtask => 
    subtask.id === subtaskId 
      ? { ...subtask, ...updates } 
      : subtask
  );
  
  // Update the todo with the modified subtasks
  return updateTodo(todoId, userId, { 
    subtasks: updatedSubtasks,
    // If all subtasks are completed, mark the todo as completed
    completed: updatedSubtasks.every(st => st.completed)
  });
}

export async function getTodosByCategory(
  userId: string,
  category: string
): Promise<Todo[]> {
  console.log(`Getting todos for user ${userId} and category ${category}`);
  const cursor = await todos.find({ userId, category });
  const results = await cursor.toArray();
  console.log(`Found ${results.length} todos for category ${category}`);
  return results;
}

export async function getOverdueTodos(userId: string): Promise<Todo[]> {
  console.log(`Getting overdue todos for user ${userId}`);
  const now = new Date();
  console.log(`Current date: ${now.toISOString()}`);
  const cursor = await todos.find({
    userId,
    completed: false,
    dueDate: { $lt: now }
  }, {
    sort: { dueDate: 1 } // Sort by due date ascending (oldest first)
  });
  const results = await cursor.toArray();
  console.log(`Found ${results.length} overdue todos`);
  return results;
}

export async function getTodoStats(userId: string): Promise<{
  total: number;
  completed: number;
  overdue: number;
  highPriority: number;
  byCategory: Record<string, number>;
}> {
  const cursor = await todos.find({ userId });
  const allTodos = await cursor.toArray();
  const now = new Date();
  
  const stats = {
    total: allTodos.length,
    completed: 0,
    overdue: 0,
    highPriority: 0,
    byCategory: {} as Record<string, number>
  };
  
  for (const todo of allTodos) {
    // Count completed
    if (todo.completed) {
      stats.completed++;
    }
    
    // Count overdue
    if (!todo.completed && todo.dueDate && todo.dueDate < now) {
      stats.overdue++;
    }
    
    // Count high priority
    if (todo.priority === "high") {
      stats.highPriority++;
    }
    
    // Count by category
    if (todo.category) {
      stats.byCategory[todo.category] = (stats.byCategory[todo.category] || 0) + 1;
    }
  }
  
  return stats;
}

export async function bulkUpdateTodos(
  userId: string,
  filter: Partial<Todo>,
  updates: Partial<Omit<Todo, "_id" | "userId" | "createdAt">>
): Promise<number> {
  // Combine user filter with userId for security
  const fullFilter = { ...filter, userId };
  
  const updateDoc = {
    ...updates,
    updatedAt: new Date()
  };
  
  const result = await todos.updateMany(fullFilter, { $set: updateDoc });
  return result.modifiedCount;
} 