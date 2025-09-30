import { ObjectId } from "bson";

type SortDirection = 1 | -1;

/**
 * Represents the result of an update operation.
 *
 * @template T The document type that was updated
 */
interface UpdateResult<T> {
  /** Number of documents that matched the filter */
  matchedCount: number;
  /** Number of documents that were modified */
  modifiedCount: number;
  /** The ID of the document that was upserted, or null if no upsert occurred */
  upsertedId: ObjectId | null;
  /** Number of documents that were upserted (0 or 1) */
  upsertedCount: number;
  /** Whether the operation was acknowledged */
  acknowledged: boolean;
  /** Whether the operation encountered write errors */
  hasWriteErrors?: boolean;
  /** Details of any write errors that occurred */
  writeErrors?: { index: number; error: Error }[];
}

type KvKeyPart = string | number | Uint8Array;
type KvKey = readonly [string, KvKeyPart, ...KvKeyPart[]];

/**
 * Options for find operations.
 *
 * @template T The document type being queried
 */
interface FindOptions<T = Document> {
  /** Sorting criteria (field name to sort direction mapping) */
  sort?: Record<string, SortDirection>;
  /** Maximum number of documents to return */
  limit?: number;
  /** Number of documents to skip */
  skip?: number;
  /** Fields to include or exclude in the result */
  projection?: Record<string, 0 | 1 | boolean>;
}

/**
 * Options for insertMany operations.
 */
interface InsertManyOptions {
  /**
   * Whether to stop processing on the first error.
   * If false, continues inserting remaining documents even if some fail.
   */
  ordered?: boolean;
}

/**
 * Result of an insertMany operation.
 */
interface InsertManyResult {
  /** Number of documents that were inserted */
  insertedCount: number;
  /** Array of IDs for the inserted documents */
  insertedIds: ObjectId[];
  /** Whether the operation encountered write errors */
  hasWriteErrors: boolean;
  /** Details of any write errors that occurred */
  writeErrors?: { index: number; error: Error }[];
}

/**
 * Result of an insertOne operation.
 */
interface InsertOneResult {
  /** Whether the operation was acknowledged */
  acknowledged: boolean;
  /** The ID of the inserted document */
  insertedId: ObjectId;
}

/**
 * Options for delete operations.
 */
interface DeleteOptions {
  // Future options like collation, hint can be added here
}

/**
 * Result of a delete operation.
 */
interface DeleteResult {
  /** Whether the operation was acknowledged */
  acknowledged: boolean;
  /** Number of documents that were deleted */
  deletedCount: number;
}

/**
 * Options for count operations.
 */
interface CountOptions {
  /** Maximum number of documents to count */
  limit?: number;
  /** Number of documents to skip */
  skip?: number;
  // Future options like hint, maxTimeMS can be added here
}

/**
 * Options for distinct operations.
 */
interface DistinctOptions {
  // Future options like maxTimeMS, collation can be added here
}

/**
 * Comparison operators for query filters.
 *
 * @template T The type of the field being compared
 */
type ComparisonOperator<T> = {
  /** Matches values equal to the specified value */
  $eq?: T;
  /** Matches values greater than the specified value */
  $gt?: T;
  /** Matches values greater than or equal to the specified value */
  $gte?: T;
  /** Matches values less than the specified value */
  $lt?: T;
  /** Matches values less than or equal to the specified value */
  $lte?: T;
  /** Matches values not equal to the specified value */
  $ne?: T;
  /** Matches values in the specified array */
  $in?: T[];
  /** Matches values not in the specified array */
  $nin?: T[];
};

/**
 * Logical operators for query filters.
 *
 * @template T The document type being filtered
 */
type LogicalOperator<T> = {
  /** Joins query clauses with a logical AND */
  $and?: Filter<T>[];
  /** Joins query clauses with a logical OR */
  $or?: Filter<T>[];
  /** Joins query clauses with a logical NOR */
  $nor?: Filter<T>[];
  /** Inverts the effect of a query expression */
  $not?: Filter<T>;
};

/**
 * Array operators for query filters.
 *
 * @template T The type of the array field
 */
type ArrayOperator<T> = {
  /** Matches arrays that contain all specified elements */
  $all?: T[];
  /** Matches arrays that contain at least one element matching all the specified conditions */
  $elemMatch?: Filter<T>;
  /** Matches arrays with the specified number of elements */
  $size?: number;
};

/**
 * Element operators for query filters.
 */
type ElementOperator = {
  /** Matches documents that have the specified field */
  $exists?: boolean;
  /** Matches documents where the value of a field is of the specified type */
  $type?: string;
};

/**
 * Represents a MongoDB-style query filter.
 *
 * Filters can include direct field comparisons, comparison operators,
 * array operators, element operators, and logical operators.
 *
 * @example
 * ```ts
 * // Simple equality filter
 * const filter: Filter<User> = { name: "John" };
 *
 * // Comparison operator
 * const filter: Filter<User> = { age: { $gt: 25 } };
 *
 * // Logical operator
 * const filter: Filter<User> = {
 *   $or: [
 *     { name: "John" },
 *     { name: "Jane" }
 *   ]
 * };
 *
 * // Array operator
 * const filter: Filter<User> = { tags: { $all: ["developer", "deno"] } };
 * ```
 *
 * @template T The document type to filter
 */
type Filter<T = any> =
  & {
    [P in keyof T & string]?:
      | T[P]
      | ComparisonOperator<T[P]>
      | ArrayOperator<T[P]>
      | ElementOperator;
  }
  & LogicalOperator<T>;

/**
 * Represents a document stored in a collection.
 *
 * All documents must have an `_id` field of type ObjectId.
 * Additional fields can be of any type and are defined by the generic type parameter.
 *
 * @example
 * ```ts
 * interface User extends Document {
 *   name: string;
 *   email: string;
 *   age: number;
 * }
 * ```
 */
export interface Document {
  _id: ObjectId;
  [key: string]: unknown;
}

// Update existing interfaces to use new types
interface FindOptions<T = Document> {
  sort?: Record<string, SortDirection>;
  limit?: number;
  skip?: number;
  projection?: Record<string, 0 | 1 | boolean>;
}

/**
 * Update operators for modifying documents.
 *
 * @template T The document type being updated
 */
type UpdateOperator<T> = {
  /** Sets the value of specified fields */
  $set?: Partial<T>;
  /** Removes specified fields */
  $unset?: Partial<Record<string, true>>;
  /** Increments the value of numeric fields */
  $inc?: Partial<Record<string, number>>;
  /** Multiplies the value of numeric fields */
  $mul?: Partial<Record<string, number>>;
  /** Updates fields with specified value if it's less than the current value */
  $min?: Partial<Record<string, number | Date>>;
  /** Updates fields with specified value if it's greater than the current value */
  $max?: Partial<Record<string, number | Date>>;
  /** Adds elements to array fields */
  $push?: Partial<Record<string, unknown>>;
  /** Removes elements from array fields that match a condition */
  $pull?: Partial<Record<string, unknown>>;
  /** Adds elements to array fields only if they don't already exist */
  $addToSet?: Partial<Record<string, unknown>>;
};

/**
 * Options for update operations.
 *
 * @template T The document type being updated
 */
interface UpdateOptions<T> {
  /**
   * Whether to insert a document if no documents match the filter.
   * Default is false.
   */
  upsert?: boolean;
  /**
   * Array filters for updating elements in arrays.
   * Used with positional operators in update expressions.
   */
  arrayFilters?: ArrayFilter<T>[];
  /**
   * Whether to stop processing on the first error.
   * If false, continues updating remaining documents even if some fail.
   */
  ordered?: boolean;
}

/**
 * Type for array filters used in update operations.
 *
 * @template T The document type being updated
 */
type ArrayFilter<T> = {
  [key: string]: Filter<T>;
};

/**
 * Type that adds an _id field to a document type.
 *
 * @template T The document type
 */
type WithId<T> = T & { _id: ObjectId };

/**
 * Options for index creation.
 */
interface IndexOptions {
  /** Whether the index should enforce uniqueness */
  unique?: boolean;
  /** Whether the index should only include documents that contain the indexed field */
  sparse?: boolean;
  /** Custom name for the index */
  name?: string;
}

/**
 * Definition of an index to be created.
 *
 * @example
 * ```ts
 * // Simple ascending index on a single field
 * const indexDef: IndexDefinition = { key: { email: 1 } };
 *
 * // Compound index with descending order on age and ascending on name
 * const compoundIndex: IndexDefinition = {
 *   key: { age: -1, name: 1 },
 *   options: { unique: true }
 * };
 * ```
 */
interface IndexDefinition {
  /**
   * Fields to index and their sort direction.
   * Use 1 for ascending order, -1 for descending order.
   */
  key: Record<string, 1 | -1>;
  /** Additional options for the index */
  options?: IndexOptions;
}

/**
 * Information about an existing index.
 */
interface IndexInfo {
  /** The index definition */
  spec: IndexDefinition;
  /** Options that were used when creating the index */
  options: IndexOptions;
}

/**
 * Utility functions for working with nested objects
 */
const utils = {
  getNestedValue(obj: any, path: string): unknown {
    if (!obj) return undefined;

    return path.split(".").reduce((current, part) => {
      if (current === undefined || current === null) return undefined;

      if (Array.isArray(current)) {
        // For array fields, return the array if we're accessing the array itself
        // This is important for array operators like $all, $size, etc.
        if (part === "$" || part === "") return current;

        // For array fields with numeric index
        if (/^\d+$/.test(part)) {
          const index = parseInt(part, 10);
          return index < current.length ? current[index] : undefined;
        }

        // For array of objects, collect all matching values
        // This handles cases like: { "tags.0": "a" } or { "items.name": "foo" }
        if (current.some((item) => item && typeof item === "object")) {
          const values = current
            .filter((item) => item && typeof item === "object")
            .map((item) => item[part])
            .filter((v) => v !== undefined);

          return values.length ? values : undefined;
        }

        return undefined;
      }

      // Handle nested object access
      return current && typeof current === "object" ? current[part] : undefined;
    }, obj);
  },

  setNestedValue(obj: any, path: string, value: unknown): void {
    const parts = path.split(".");
    let current = obj;

    for (let i = 0; i < parts.length - 1; i++) {
      const part = parts[i];
      if (!(part in current)) {
        current[part] = {};
      }
      current = current[part];
    }

    const lastPart = parts[parts.length - 1];
    current[lastPart] = value;
  },

  deleteNestedField(obj: any, path: string): void {
    const parts = path.split(".");
    let current = obj;

    for (let i = 0; i < parts.length - 1; i++) {
      const part = parts[i];
      if (!(part in current) || current[part] === null) {
        return; // Field doesn't exist, nothing to delete
      }
      current = current[part];
    }

    delete current[parts[parts.length - 1]];
  }
};

/**
 * Cursor for iterating over query results.
 * Provides MongoDB-compatible cursor interface.
 */
export class Cursor<T extends Document> {
  private documents: WithId<T>[];

  constructor(documents: WithId<T>[]) {
    this.documents = documents;
  }

  /**
   * Returns all the documents in the cursor as an array.
   */
  async toArray(): Promise<WithId<T>[]> {
    return this.documents;
  }

  /**
   * Returns the number of documents in the cursor.
   */
  async count(): Promise<number> {
    return this.documents.length;
  }

  /**
   * Makes the cursor iterable.
   */
  [Symbol.iterator](): Iterator<WithId<T>> {
    return this.documents[Symbol.iterator]();
  }

  /**
   * Returns the first document in the cursor, or null if the cursor is empty.
   */
  async next(): Promise<WithId<T> | null> {
    return this.documents.length > 0 ? this.documents[0] : null;
  }

  /**
   * Returns a new cursor with the specified limit.
   */
  limit(limit: number): Cursor<T> {
    return new Cursor<T>(this.documents.slice(0, limit));
  }

  /**
   * Returns a new cursor that skips the specified number of documents.
   */
  skip(skip: number): Cursor<T> {
    return new Cursor<T>(this.documents.slice(skip));
  }

  /**
   * Returns a new cursor with the specified sort order.
   */
  sort(sortOptions: Record<string, SortDirection>): Cursor<T> {
    // Create a copy of the documents array to avoid modifying the original
    const sortedDocs = [...this.documents];
    
    // Sort the documents based on the provided sort options
    sortedDocs.sort((a, b) => {
      for (const [field, direction] of Object.entries(sortOptions)) {
        const aValue = utils.getNestedValue(a, field);
        const bValue = utils.getNestedValue(b, field);
        
        if (aValue === bValue) continue;
        
        // Handle null/undefined values
        if (aValue == null && bValue != null) return direction;
        if (aValue != null && bValue == null) return -direction;
        
        // Compare values based on their types
        if (aValue instanceof Date && bValue instanceof Date) {
          return (aValue.getTime() - bValue.getTime()) * direction;
        }
        
        if (typeof aValue === 'string' && typeof bValue === 'string') {
          return aValue.localeCompare(bValue) * direction;
        }
        
        if (typeof aValue === 'number' && typeof bValue === 'number') {
          return (aValue - bValue) * direction;
        }
        
        // Default comparison for other types
        return ((aValue as any) > (bValue as any) ? 1 : -1) * direction;
      }
      return 0;
    });
    
    return new Cursor<T>(sortedDocs);
  }

  /**
   * Returns a new cursor with the specified projection.
   */
  project(projection: Record<string, 0 | 1 | boolean>): Cursor<T> {
    const projectedDocs = this.documents.map(doc => {
      const result: any = { _id: doc._id };
      const includeMode = this.isIncludeProjection(projection);
      
      if (includeMode) {
        // Include only specified fields
        for (const [field, include] of Object.entries(projection)) {
          if (field === '_id') continue; // _id is always included unless explicitly excluded
          if (include) {
            const value = utils.getNestedValue(doc, field);
            if (value !== undefined) {
              utils.setNestedValue(result, field, value);
            }
          }
        }
      } else {
        // Include all fields except those specified
        Object.assign(result, doc);
        for (const [field, exclude] of Object.entries(projection)) {
          if (field !== '_id' || exclude) { // Only exclude _id if explicitly set to 0
            utils.deleteNestedField(result, field);
          }
        }
      }
      
      return result as WithId<T>;
    });
    
    return new Cursor<T>(projectedDocs);
  }

  private isIncludeProjection(projection: Record<string, 0 | 1 | boolean>): boolean {
    // Determine if this is an include (1) or exclude (0) projection
    // MongoDB rule: can't mix include and exclude except for _id
    let includeMode: boolean | null = null;
    
    for (const [field, value] of Object.entries(projection)) {
      if (field === '_id') continue; // _id can be either included or excluded regardless
      
      if (includeMode === null) {
        includeMode = Boolean(value);
      } else if (Boolean(value) !== includeMode) {
        throw new Error("Projection cannot have a mix of inclusion and exclusion.");
      }
    }
    
    return includeMode !== false; // Default to include mode if no fields specified besides _id
  }
}

/**
 * Represents a collection of documents in the database.
 *
 * A collection is a group of documents that share a common structure,
 * similar to a table in a relational database.
 *
 * @template T The document type stored in this collection
 */
class Collection<T extends Document> {
  private kv: Deno.Kv;
  private collectionName: string;

  constructor(kv: Deno.Kv, collectionName: string) {
    this.kv = kv;
    this.collectionName = collectionName;
  }

  // MongoDB-like methods implemented:
  // - findOne(filter: object)
  // - find(filter: object)
  // - insertOne(document: object)
  // - insertMany(documents: object[])
  // - updateOne(filter: object, update: object)
  // - deleteOne(filter: object)
  // - deleteMany(filter: object)

  private getKvKey(id: ObjectId): KvKey {
    return [this.collectionName, id.toString()] as const;
  }

  private generateId(): ObjectId {
    return new ObjectId();
  }

  private serializeObjectId(id: ObjectId): string {
    return id.toString();
  }

  private deserializeObjectId(id: string | Uint8Array): ObjectId {
    return new ObjectId(id);
  }

  private getNestedValue(obj: any, path: string): unknown {
    return utils.getNestedValue(obj, path);
  }

  private setNestedValue(obj: any, path: string, value: unknown): void {
    utils.setNestedValue(obj, path, value);
  }

  private deleteNestedField(obj: any, path: string): void {
    utils.deleteNestedField(obj, path);
  }

  private async checkIndexViolations(
    doc: Omit<T, "_id"> & { _id: string },
  ): Promise<void> {
    const indexPrefix = ["__indexes__", this.collectionName] as const;
    for await (const entry of this.kv.list({ prefix: indexPrefix })) {
      const indexInfo = entry.value as {
        spec: IndexDefinition;
        options: IndexOptions;
      };

      if (indexInfo.options.unique) {
        const fields = Object.keys(indexInfo.spec.key);

        for (const field of fields) {
          const value = this.getNestedValue(doc, field);
          const serializedValue = this.serializeIndexValue(value);

          const prefix = [
            this.collectionName,
            "__idx__",
            field,
            serializedValue,
          ] as const;

          // Check for existing documents with this value
          for await (const existing of this.kv.list({ prefix })) {
            const existingId = (existing.value as any)._id;
            // Only throw if it's a different document
            if (existingId && existingId !== doc._id.toString()) {
              throw new Error(`Duplicate key error: ${field}`);
            }
          }
        }
      }
    }
  }

  /**
   * Inserts a single document into the collection.
   *
   * If the document doesn't have an `_id` field, one will be generated automatically.
   *
   * @example
   * ```ts
   * const result = await users.insertOne({
   *   name: "John Doe",
   *   email: "john@example.com",
   *   age: 30
   * });
   * console.log(result.insertedId); // ObjectId("...")
   * ```
   *
   * @param doc The document to insert
   * @returns A promise that resolves to an InsertOneResult
   * @throws If a document with the same _id already exists
   */
  async insertOne(
    doc: Omit<T, "_id"> & { _id?: ObjectId },
  ): Promise<InsertOneResult> {
    if (!doc || typeof doc !== "object") {
      throw new Error("Invalid document");
    }

    const _id = doc._id || new ObjectId();
    const key = this.getKvKey(_id);
    const docToInsert = { ...doc, _id: this.serializeObjectId(_id) };

    // Check for existing document
    const existing = await this.kv.get(key);
    if (existing.value) {
      throw new Error("Duplicate key error");
    }

    // Update all indexes first
    const indexPrefix = ["__indexes__", this.collectionName] as const;
    for await (const entry of this.kv.list({ prefix: indexPrefix })) {
      const indexInfo = entry.value as IndexInfo;
      await this.updateIndexEntry(
        { ...doc, _id } as Omit<T, "_id"> & { _id: ObjectId },
        indexInfo.spec,
        indexInfo.options,
      );
    }

    // Then insert the document
    const result = await this.kv.atomic()
      .check({ key, versionstamp: null })
      .set(key, docToInsert)
      .commit();

    if (!result.ok) {
      throw new Error("Failed to insert document");
    }

    return {
      acknowledged: true,
      insertedId: _id,
    };
  }

  // Add helper method to validate document fields
  private validateDocument(doc: any): void {
    // Validate each field recursively
    const validateField = (value: unknown): void => {
      if (value === null) return;

      if (Array.isArray(value)) {
        value.forEach(validateField);
        return;
      }

      if (value instanceof Date) {
        if (isNaN(value.getTime())) {
          throw new Error("Invalid Date value");
        }
        return;
      }

      if (value instanceof ObjectId) {
        // ObjectId is already validated by its constructor
        return;
      }

      if (typeof value === "object" && value !== null) {
        Object.values(value).forEach(validateField);
        return;
      }

      // Basic types are always valid
      if (["string", "number", "boolean", "undefined"].includes(typeof value)) {
        return;
      }

      // If we get here, we have an invalid type
      throw new Error(`Invalid field type: ${typeof value}`);
    };

    validateField(doc);
  }

  private applyProjection<D extends Document>(
    doc: D,
    projection?: Record<string, number | boolean>,
  ): WithId<T> {
    if (!projection || Object.keys(projection).length === 0) {
      return doc as unknown as WithId<T>;
    }

    // Check for mixed inclusion/exclusion
    const values = Object.entries(projection)
      .filter(([key]) => key !== "_id")
      .map(([_, value]) => value);

    if (
      values.length > 0 && values.some((v) => v === 1) &&
      values.some((v) => v === 0)
    ) {
      throw new Error(
        "Projection cannot have a mix of inclusion and exclusion",
      );
    }

    const result: Record<string, unknown> = { _id: doc._id };
    const includeMode = values.some((v) => v === 1 || v === true);

    if (includeMode) {
      // Include mode: only add specified fields
      Object.entries(projection).forEach(([path, value]) => {
        if (value === 1 || path === "_id") {
          const val = this.getNestedValue(doc, path);
          if (val !== undefined) {
            this.setNestedValue(result, path, val);
          }
        }
      });
    } else {
      // Exclude mode: copy all fields except excluded ones
      Object.entries(doc).forEach(([key, value]) => {
        if (projection[key] !== 0 && key !== "_id") {
          result[key] = value;
        }
      });
    }

    return result as WithId<T>;
  }

  private isEqual(a: unknown, b: unknown): boolean {
    // Handle ObjectId comparison
    if (a instanceof ObjectId && b instanceof ObjectId) {
      return a.equals(b);
    }

    // Handle Date comparison
    if (a instanceof Date && b instanceof Date) {
      return a.getTime() === b.getTime();
    }

    // Handle RegExp comparison
    if (a instanceof RegExp && b instanceof RegExp) {
      return a.toString() === b.toString();
    }

    // Handle primitive types
    if (a === b) return true;

    // Handle null/undefined
    if (a == null || b == null) return false;

    // Handle different types
    if (typeof a !== typeof b) return false;

    // Handle arrays
    if (Array.isArray(a) && Array.isArray(b)) {
      if (a.length !== b.length) return false;

      // For arrays, we need to check if all elements in a exist in b
      // This is to support MongoDB's array equality semantics
      if (a.length === 0) return true;

      // Check if arrays contain exactly the same elements in the same order
      return a.every((item, index) => this.isEqual(item, b[index]));
    }

    // Handle objects
    if (typeof a === "object" && typeof b === "object") {
      const keysA = Object.keys(a as object);
      const keysB = Object.keys(b as object);

      if (keysA.length !== keysB.length) return false;

      return keysA.every((key) =>
        Object.prototype.hasOwnProperty.call(b, key) &&
        this.isEqual((a as any)[key], (b as any)[key])
      );
    }

    return false;
  }

  private matchesCondition(value: unknown, condition: unknown): boolean {
    // Special handling for ObjectId
    if (value instanceof ObjectId && condition instanceof ObjectId) {
      return value.equals(condition);
    }

    // Handle array value matching
    if (
      Array.isArray(value) && !Array.isArray(condition) &&
      !(condition && typeof condition === "object" &&
        ("$all" in condition || "$size" in condition ||
          "$elemMatch" in condition || "$nin" in condition))
    ) {
      return value.some((item) => this.matchesCondition(item, condition));
    }

    // Handle exact array matching
    if (Array.isArray(value) && Array.isArray(condition)) {
      if (value.length !== condition.length) return false;
      return condition.every((item, index) => this.isEqual(value[index], item));
    }

    if (condition && typeof condition === "object") {
      return Object.entries(condition as Record<string, unknown>).every(
        ([op, val]) => {
          switch (op) {
            case "$eq":
              return this.isEqual(value, val);
            case "$gt":
              if (!this.isComparable(value) || !this.isComparable(val)) {
                return false;
              }
              return value > val;
            case "$gte":
              if (!this.isComparable(value) || !this.isComparable(val)) {
                return false;
              }
              return value >= val;
            case "$lt":
              if (!this.isComparable(value) || !this.isComparable(val)) {
                return false;
              }
              return value < val;
            case "$lte":
              if (!this.isComparable(value) || !this.isComparable(val)) {
                return false;
              }
              return value <= val;
            case "$ne":
              return !this.isEqual(value, val);
            case "$in":
              return Array.isArray(val) &&
                val.some((v) => this.isEqual(value, v));
            case "$nin":
              // For array values, $nin should only match if none of the array elements match any of the values
              if (Array.isArray(value)) {
                return Array.isArray(val) &&
                  !value.some(item => val.some(v => this.isEqual(item, v)));
              }
              // For non-array values, $nin should match if the value is not in the array
              return Array.isArray(val) &&
                !val.some((v) => this.isEqual(value, v));
            case "$exists":
              return (val as boolean) === (value !== undefined);
            case "$all":
              if (!Array.isArray(value) || !Array.isArray(val)) return false;
              return val.every((v) =>
                value.some((item) => this.isEqual(item, v))
              );
            case "$size":
              return Array.isArray(value) && value.length === val;
            case "$elemMatch":
              if (!Array.isArray(value)) return false;
              return value.some((item) => {
                if (typeof val !== "object") return this.isEqual(item, val);
                return this.matchesCondition(item, val);
              });
            case "$not":
              // $not negates the result of the nested condition
              if (typeof val !== "object") return !this.isEqual(value, val);
              return !this.matchesCondition(value, val);
            case "$type":
              const type = typeof value;
              if (val === "null") return value === null;
              if (val === "array") return Array.isArray(value);
              if (val === "date") return value instanceof Date;
              if (val === "objectId") return value instanceof ObjectId;
              return type === val;
            default:
              // If it's not a recognized operator, treat it as a field path
              if (
                typeof value === "object" && value !== null &&
                !Array.isArray(value)
              ) {
                const nestedValue = this.getNestedValue(value, op);
                return this.matchesCondition(nestedValue, val);
              }
              return false;
          }
        },
      );
    }

    // Direct value comparison
    return this.isEqual(value, condition);
  }

  private matchesFilter(doc: WithId<T>, filter: Filter<T>): boolean {
    for (const [key, condition] of Object.entries(filter)) {
      // Special operators
      if (key === "$or" && Array.isArray(condition)) {
        if (condition.length === 0) return true; // Empty $or is always true
        if (
          !condition.some((subFilter) => this.matchesFilter(doc, subFilter))
        ) {
          return false;
        }
        continue;
      }

      if (key === "$and" && Array.isArray(condition)) {
        if (condition.length === 0) return true; // Empty $and is always true
        if (
          !condition.every((subFilter) => this.matchesFilter(doc, subFilter))
        ) {
          return false;
        }
        continue;
      }

      if (key === "$nor" && Array.isArray(condition)) {
        if (condition.length === 0) return true; // Empty $nor is always true
        if (condition.some((subFilter) => this.matchesFilter(doc, subFilter))) {
          return false;
        }
        continue;
      }

      if (key === "$not" && condition && typeof condition === "object") {
        if (this.matchesFilter(doc, condition as Filter<T>)) {
          return false;
        }
        continue;
      }

      // Regular field conditions
      const value = this.getNestedValue(doc, key);

      if (condition === null) {
        if (value !== null) return false;
        continue;
      }

      if (
        typeof condition !== "object" || condition instanceof ObjectId ||
        Array.isArray(condition)
      ) {
        // Direct comparison
        if (!this.matchesCondition(value, condition)) return false;
        continue;
      }

      // Object condition (operators)
      if (!this.matchesCondition(value, condition)) {
        return false;
      }
    }

    return true;
  }

  // Helper method to compare values with proper ObjectId handling
  private compareValues(a: any, b: any): boolean {
    if (a instanceof ObjectId && b instanceof ObjectId) {
      return a.toString() === b.toString();
    }

    if (a instanceof Date && b instanceof Date) {
      return a.getTime() === b.getTime();
    }

    return a === b;
  }

  /**
   * Finds a single document that matches the filter.
   *
   * @example
   * ```ts
   * const user = await users.findOne({ email: "john@example.com" });
   * if (user) {
   *   console.log(user.name); // "John Doe"
   * }
   * ```
   *
   * @param filter The query filter
   * @param options Options for the find operation
   * @returns A promise that resolves to the matching document, or null if none is found
   */
  async findOne(
    filter: Filter<T>,
    options: FindOptions<T> = {},
  ): Promise<WithId<T> | null> {
    if ("_id" in filter && filter._id instanceof ObjectId) {
      const key = this.getKvKey(filter._id);
      const result = await this.kv.get(key);
      if (!result.value) return null;

      // Reconstruct the document with proper ObjectId instance
      const doc = result.value as WithId<T>;
      doc._id = this.deserializeObjectId(doc._id as unknown as string);
      return doc;
    }

    const cursor = await this.find(filter, { limit: 1, ...options });
    const results = await cursor.toArray();
    return results[0] || null;
  }

  private async findUsableIndex(filter: Filter<T>): Promise<IndexInfo | null> {
    // Get all indexes
    const indexPrefix = ["__indexes__", this.collectionName] as const;

    for await (const entry of this.kv.list({ prefix: indexPrefix })) {
      const indexInfo = entry.value as IndexInfo;

      // Check if index fields match query fields
      if (this.canUseIndexForQuery(indexInfo.spec, filter)) {
        return indexInfo;
      }
    }

    return null;
  }

  private canUseIndexForQuery(
    indexSpec: IndexDefinition,
    filter: Filter<T>,
  ): boolean {
    const indexFields = Object.keys(indexSpec.key);

    // Simple case: single field exact match or range
    if (indexFields.length === 1) {
      const field = indexFields[0];
      return field in filter && (
        // Simple equality
        typeof filter[field] !== "object" ||
        // Range query
        (typeof filter[field] === "object" &&
          filter[field] !== null &&
          Object.keys(filter[field] as object).some((op) =>
            ["$eq", "$gt", "$gte", "$lt", "$lte", "$in"].includes(op)
          ))
      );
    }

    // Compound index: first field must be exact match, others can be range
    const [firstField, ...restFields] = indexFields;
    if (!(firstField in filter)) {
      return false;
    }

    // Check remaining fields exist in filter
    return restFields.every((field) => field in filter);
  }

  private isIndexableCondition(condition: unknown): boolean {
    if (condition === null) return true;

    if (typeof condition === "object") {
      // Check for comparison operators
      const ops = Object.keys(condition as object);
      const validOps = ["$eq", "$gt", "$gte", "$lt", "$lte", "$in"];
      return ops.every((op) => validOps.includes(op));
    }

    // Direct value comparison
    return true;
  }

  private async findUsingIndex(
    indexInfo: IndexInfo,
    filter: Filter<T>,
    options: FindOptions<T>,
  ): Promise<WithId<T>[]> {
    const seenIds = new Set<string>();
    const results: WithId<T>[] = [];

    const addUniqueDoc = (doc: WithId<T>) => {
      const idStr = doc._id.toString();
      if (!seenIds.has(idStr)) {
        seenIds.add(idStr);
        results.push(doc);
      }
    };

    const indexFields = Object.keys(indexInfo.spec.key);

    if (indexFields.length === 1) {
      const field = indexFields[0];
      const condition = filter[field];

      if (this.isRangeQuery(condition)) {
        return this.findUsingIndexRange(
          field,
          condition as any,
          filter,
          options,
        );
      }

      const value = this.getQueryValue(condition);
      const serializedValue = this.serializeIndexValue(value);

      const prefix = [
        this.collectionName,
        "__idx__",
        field,
        serializedValue,
      ] as const;

      for await (const entry of this.kv.list({ prefix })) {
        const docId = (entry.value as any)._id;
        const doc = await this.findOne(
          { _id: new ObjectId(docId) } as Filter<T>,
        );

        if (doc && this.matchesFilter(doc, filter)) {
          addUniqueDoc(doc);
        }
      }
    } else {
      const prefixParts = [this.collectionName, "__idx__"];
      const firstField = indexFields[0];
      const firstValue = filter[firstField];

      // Start with first field exact match
      const prefix = [
        ...prefixParts,
        firstField,
        String(this.serializeIndexValue(firstValue)),
      ] as const;

      for await (const entry of this.kv.list({ prefix })) {
        const docId = (entry.value as any)._id;
        const doc = await this.findOne(
          { _id: new ObjectId(docId) } as Filter<T>,
        );
        if (doc && this.matchesFilter(doc, filter)) {
          addUniqueDoc(doc);
        }
      }
    }

    const finalResults = this.applyFindOptions(results, options);
    return finalResults;
  }

  private async findUsingIndexRange(
    field: string,
    condition: Record<string, any>,
    fullFilter: Filter<T>,
    options: FindOptions<T>,
  ): Promise<WithId<T>[]> {
    const start = condition.$gt || condition.$gte;
    const end = condition.$lt || condition.$lte;
    const sortDir = options.sort?.[field] === -1 ? -1 : 1;

    // For debugging
    console.log("Range query:", { field, condition, sortDir });

    // Try a different approach - use a prefix query instead of a range query
    // This is more reliable with Deno KV
    const prefix = [this.collectionName, "__idx__", field] as const;
    const seenIds = new Set<string>();
    const results: WithId<T>[] = [];

    try {
      // Get all index entries for this field
      for await (const entry of this.kv.list({ prefix })) {
        // Extract the value from the key
        const indexValue = entry.key[3];

        // Check if the value matches our condition
        let matches = true;

        if (start !== undefined) {
          const startOp = condition.$gt ? ">" : ">=";
          const startValue = this.serializeIndexValue(start);

          // Convert both to strings for comparison
          const strIndexValue = String(indexValue);
          const strStartValue = String(startValue);

          if (startOp === ">" && strIndexValue <= strStartValue) {
            matches = false;
          }
          if (startOp === ">=" && strIndexValue < strStartValue) {
            matches = false;
          }
        }

        if (end !== undefined && matches) {
          const endOp = condition.$lt ? "<" : "<=";
          const endValue = this.serializeIndexValue(end);

          // Convert both to strings for comparison
          const strIndexValue = String(indexValue);
          const strEndValue = String(endValue);

          if (endOp === "<" && strIndexValue >= strEndValue) matches = false;
          if (endOp === "<=" && strIndexValue > strEndValue) matches = false;
        }

        if (matches) {
          const docId = (entry.value as any)._id;
          const doc = await this.findOne(
            { _id: new ObjectId(docId) } as Filter<T>,
          );

          if (doc && this.matchesFilter(doc, fullFilter)) {
            const idStr = doc._id.toString();
            if (!seenIds.has(idStr)) {
              seenIds.add(idStr);
              results.push(doc);
            }
          }
        }
      }
    } catch (error) {
      console.error("Error in range query:", { error, field, condition });
      // Fall back to full scan
      return this.findWithoutIndex(fullFilter, options);
    }

    // Apply sort and other options
    return this.applyFindOptions(results, options);
  }

  private isRangeQuery(condition: unknown): boolean {
    if (typeof condition !== "object" || condition === null) return false;
    const ops = Object.keys(condition as object);
    return ops.some((op) => ["$gt", "$gte", "$lt", "$lte"].includes(op));
  }

  private getQueryValue(condition: unknown): unknown {
    if (condition === null || typeof condition !== "object") {
      return condition;
    }

    const obj = condition as Record<string, unknown>;
    if ("$eq" in obj) return obj.$eq;
    if ("$in" in obj) return (obj.$in as unknown[])[0]; // Use first value for index

    return condition;
  }

  /**
   * Finds all documents that match the filter.
   *
   * @example
   * ```ts
   * const youngUsers = await users.find(
   *   { age: { $lt: 30 } },
   *   { sort: { name: 1 }, limit: 10 }
   * );
   * ```
   *
   * @param filter The query filter
   * @param options Options for the find operation
   * @returns A promise that resolves to an array of matching documents
   */
  async find(
    filter: Filter<T>,
    options: FindOptions<T> = {},
  ): Promise<Cursor<T>> {
    // Check if we can use an index
    const usableIndex = await this.findUsableIndex(filter);

    let results: WithId<T>[];
    if (usableIndex) {
      results = await this.findUsingIndex(usableIndex, filter, options);
    } else {
      // Fall back to full collection scan
      results = await this.findWithoutIndex(filter, options);
    }

    return new Cursor<T>(results);
  }

  // Rename the existing find implementation
  private async findWithoutIndex(
    filter: Filter<T>,
    options: FindOptions<T>,
  ): Promise<WithId<T>[]> {
    const results: WithId<T>[] = [];
    const prefix = [this.collectionName];

    for await (const entry of this.kv.list({ prefix })) {
      if (entry.key.length !== 2) continue; // Skip index entries

      const doc = entry.value as WithId<T>;
      doc._id = this.deserializeObjectId(doc._id as unknown as string);

      if (this.matchesFilter(doc, filter)) {
        results.push(doc);
      }
    }

    return this.applyFindOptions(results, options);
  }

  private sortDocuments(
    docs: WithId<T>[],
    sortOptions: Record<string, SortDirection>,
  ): WithId<T>[] {
    const entries = Object.entries(sortOptions);

    return [...docs].sort((a, b) => {
      for (const [field, direction] of entries) {
        const aVal = this.getNestedValue(a, field);
        const bVal = this.getNestedValue(b, field);

        if (aVal === bVal) continue;

        // Handle ObjectId comparison
        if (aVal instanceof ObjectId && bVal instanceof ObjectId) {
          return direction *
            (aVal.equals(bVal) ? 0 : aVal.id > bVal.id ? 1 : -1);
        }

        // Handle null/undefined values
        if (aVal == null) return direction;
        if (bVal == null) return -direction;

        // Compare values
        if (aVal < bVal) return -direction;
        if (aVal > bVal) return direction;
      }
      return 0;
    });
  }

  /**
   * Updates a single document that matches the filter.
   *
   * @example
   * ```ts
   * const result = await users.updateOne(
   *   { email: "john@example.com" },
   *   { $set: { age: 31 }, $push: { tags: "updated" } }
   * );
   * console.log(result.modifiedCount); // 1
   * ```
   *
   * @param filter The query filter
   * @param update The update operations to apply
   * @param options Options for the update operation
   * @returns A promise that resolves to an UpdateResult
   */
  async updateOne(
    filter: Filter<T>,
    update: UpdateOperator<T>,
    options: UpdateOptions<T> = {},
  ): Promise<UpdateResult<T>> {
    const { upsert = false } = options;
    const doc = await this.findOne(filter);

    if (!doc && upsert) {
      const _id = (filter._id as ObjectId) || this.generateId();
      const emptyDoc = { _id } as T;

      // Apply updates to empty doc
      const newDoc = this.applyUpdateOperators(emptyDoc, update);
      const key = this.getKvKey(_id);

      const result = await this.kv.atomic()
        .check({ key, versionstamp: null })
        .set(key, { ...newDoc, _id: this.serializeObjectId(_id) })
        .commit();

      if (!result.ok) {
        throw new Error("Failed to upsert document");
      }

      return {
        matchedCount: 0,
        modifiedCount: 1,
        upsertedId: _id,
        upsertedCount: 1,
        acknowledged: true,
      };
    }

    if (!doc) {
      return {
        matchedCount: 0,
        modifiedCount: 0,
        upsertedId: null,
        upsertedCount: 0,
        acknowledged: true,
      };
    }

    const updatedDoc = this.applyUpdateOperators(doc, update);
    const key = this.getKvKey(doc._id);
    const currentEntry = await this.kv.get(key);

    const result = await this.kv.atomic()
      .check({ key, versionstamp: currentEntry.versionstamp })
      .set(key, { ...updatedDoc, _id: this.serializeObjectId(doc._id) })
      .commit();

    if (!result.ok) {
      throw new Error("Failed to update document");
    }

    return {
      matchedCount: 1,
      modifiedCount: 1,
      upsertedId: null,
      upsertedCount: 0,
      acknowledged: true,
    };
  }

  private applyUpdateOperators(doc: T, update: UpdateOperator<T>): T {
    const result = { ...doc };

    // Handle $set
    if (update.$set) {
      Object.entries(update.$set).forEach(([path, value]) => {
        this.setNestedValue(result, path, value);
      });
    }

    // Handle $unset
    if (update.$unset) {
      Object.keys(update.$unset).forEach((path) => {
        const parts = path.split(".");
        let current = result;
        for (let i = 0; i < parts.length - 1; i++) {
          if (!current[parts[i]]) break;
          current = current[parts[i]] as any;
        }
        if (current && typeof current === "object") {
          delete current[parts[parts.length - 1]];
        }
      });
    }

    // Handle $inc
    if (update.$inc) {
      Object.entries(update.$inc).forEach(([path, value]) => {
        if (value !== undefined) {
          const currentValue = this.getNestedValue(result, path) as number || 0;
          this.setNestedValue(result, path, currentValue + value);
        }
      });
    }

    // Handle $mul
    if (update.$mul) {
      Object.entries(update.$mul).forEach(([path, value]) => {
        if (value !== undefined) {
          const currentValue = this.getNestedValue(result, path) as number || 0;
          this.setNestedValue(result, path, currentValue * value);
        }
      });
    }

    // Handle $push
    if (update.$push) {
      Object.entries(update.$push).forEach(([path, value]) => {
        let array = this.getNestedValue(result, path) as unknown[];
        if (!Array.isArray(array)) {
          array = [];
          this.setNestedValue(result, path, array);
        }
        array.push(value);
      });
    }

    // Handle $pull
    if (update.$pull) {
      Object.entries(update.$pull).forEach(([path, value]) => {
        const array = this.getNestedValue(result, path) as unknown[];
        if (Array.isArray(array)) {
          const filtered = array.filter((item) => !this.isEqual(item, value));
          this.setNestedValue(result, path, filtered);
        }
      });
    }

    // Handle $addToSet
    if (update.$addToSet) {
      Object.entries(update.$addToSet).forEach(([path, value]) => {
        let array = this.getNestedValue(result, path) as unknown[];
        if (!Array.isArray(array)) {
          array = [];
          this.setNestedValue(result, path, array);
        }
        // Only add the value if it doesn't already exist in the array
        if (!array.some(item => this.isEqual(item, value))) {
          array.push(value);
        }
      });
    }

    // Handle $min
    if (update.$min) {
      Object.entries(update.$min).forEach(([path, value]) => {
        if (value !== undefined) {
          const currentValue = this.getNestedValue(result, path) as number;
          if (currentValue === undefined || currentValue > (value as number)) {
            this.setNestedValue(result, path, value);
          }
        }
      });
    }

    // Handle $max
    if (update.$max) {
      Object.entries(update.$max).forEach(([path, value]) => {
        if (value !== undefined) {
          const currentValue = this.getNestedValue(result, path) as number;
          if (currentValue === undefined || currentValue < (value as number)) {
            this.setNestedValue(result, path, value);
          }
        }
      });
    }

    return result;
  }

  /**
   * Updates all documents that match the filter.
   *
   * @example
   * ```ts
   * const result = await users.updateMany(
   *   { age: { $lt: 30 } },
   *   { $set: { status: "young" } }
   * );
   * console.log(result.modifiedCount); // Number of documents updated
   * ```
   *
   * @param filter The query filter
   * @param update The update operations to apply
   * @param options Options for the update operation
   * @returns A promise that resolves to an UpdateResult
   */
  async updateMany(
    filter: Filter<T>,
    update: UpdateOperator<T>,
    options: UpdateOptions<T> = {},
  ): Promise<UpdateResult<T>> {
    const cursor = await this.find(filter);
    const docs = await cursor.toArray();
    
    // Handle upsert if no documents match and upsert is true
    if (docs.length === 0 && options.upsert) {
      return this.updateOne(filter, update, options);
    }
    
    let modifiedCount = 0;
    const writeErrors: { index: number; error: Error }[] = [];

    for (const doc of docs) {
      try {
        const updatedDoc = this.applyUpdateOperators(doc, update);
        const key = this.getKvKey(doc._id);
        const currentEntry = await this.kv.get(key);

        // Serialize ObjectId before storing
        const docToStore = {
          ...updatedDoc,
          _id: this.serializeObjectId(doc._id),
        };

        const result = await this.kv.atomic()
          .check({ key, versionstamp: currentEntry.versionstamp })
          .set(key, docToStore)
          .commit();

        if (result.ok) {
          modifiedCount++;
        }
      } catch (error) {
        writeErrors.push({ index: docs.indexOf(doc), error: error as Error });
        if (options.ordered) break;
      }
    }

    return {
      matchedCount: docs.length,
      modifiedCount,
      upsertedId: null,
      upsertedCount: 0,
      acknowledged: true,
      hasWriteErrors: writeErrors.length > 0,
      writeErrors,
    };
  }

  /**
   * Deletes a single document that matches the filter.
   *
   * @example
   * ```ts
   * const result = await users.deleteOne({ email: "john@example.com" });
   * console.log(result.deletedCount); // 1
   * ```
   *
   * @param filter The query filter
   * @returns A promise that resolves to a DeleteResult
   */
  async deleteOne(filter: Filter<T>): Promise<DeleteResult> {
    const doc = await this.findOne(filter);

    if (!doc) {
      return {
        acknowledged: true,
        deletedCount: 0,
      };
    }

    const key = this.getKvKey(doc._id);
    const currentEntry = await this.kv.get(key);

    const result = await this.kv.atomic()
      .check({ key, versionstamp: currentEntry.versionstamp })
      .delete(key)
      .commit();

    if (!result.ok) {
      throw new Error(
        "Failed to delete document - concurrent modification detected",
      );
    }

    return {
      acknowledged: true,
      deletedCount: 1,
    };
  }

  /**
   * Deletes all documents that match the filter.
   *
   * @example
   * ```ts
   * const result = await users.deleteMany({ age: { $lt: 18 } });
   * console.log(result.deletedCount); // Number of documents deleted
   * ```
   *
   * @param filter The query filter
   * @returns A promise that resolves to a DeleteResult
   */
  async deleteMany(filter: Filter<T>): Promise<DeleteResult> {
    const cursor = await this.find(filter);
    const docs = await cursor.toArray();
    
    if (docs.length === 0) {
      return {
        acknowledged: true,
        deletedCount: 0,
      };
    }
    
    let deletedCount = 0;
    const atomic = this.kv.atomic();
    
    for (const doc of docs) {
      const key = this.getKvKey(doc._id);
      const currentEntry = await this.kv.get(key);
      atomic
        .check({ key, versionstamp: currentEntry.versionstamp })
        .delete(key);
    }
    
    // Execute the atomic operation
    const result = await atomic.commit();
    
    if (!result.ok) {
      throw new Error(
        "Failed to delete documents - concurrent modification detected",
      );
    }
    
    return {
      acknowledged: true,
      deletedCount: docs.length,
    };
  }

  async countDocuments(
    filter: Filter<T> = {},
    options: CountOptions = {},
  ): Promise<number> {
    // If we have a limit or skip, we need to apply those
    if (options.limit !== undefined || options.skip !== undefined) {
      const cursor = await this.find(filter);
      const results = await cursor.toArray();
      
      const start = options.skip || 0;
      const end = options.limit !== undefined
        ? start + options.limit
        : results.length;
      
      return Math.min(Math.max(0, results.length - start), end - start);
    }

    // Otherwise, just count all matching documents
    const cursor = await this.find(filter);
    const docs = await cursor.toArray();
    return docs.length;
  }

  async estimatedDocumentCount(): Promise<number> {
    // Fast count without filtering
    let count = 0;
    const prefix = [this.collectionName];

    for await (const entry of this.kv.list({ prefix })) {
      count++;
    }

    return count;
  }

  async distinct(
    field: string,
    filter: Filter<T> = {},
    options: DistinctOptions = {},
  ): Promise<unknown[]> {
    const cursor = await this.find(filter);
    const docs = await cursor.toArray();
    
    // Extract values for the specified field
    let allValues: unknown[] = [];
    
    for (const doc of docs) {
      const value = this.getNestedValue(doc, field);
      
      if (value === undefined) continue;
      
      if (Array.isArray(value)) {
        // For array fields, add each element
        allValues = allValues.concat(value);
      } else {
        allValues.push(value);
      }
    }
    
    // Deduplicate values
    const distinctValues: unknown[] = [];
    for (const value of allValues) {
      if (!this.arrayIncludes(distinctValues, value)) {
        distinctValues.push(this.normalizeValue(value));
      }
    }
    
    return distinctValues;
  }

  private normalizeValue(value: unknown): unknown {
    // Handle special cases for comparison
    if (value === null) {
      return null;
    }

    if (typeof value === "object") {
      return JSON.stringify(value);
    }

    return value;
  }

  // Add this helper method for deep comparison
  private arrayIncludes(array: unknown[], value: unknown): boolean {
    return array.some((item) => this.isEqual(item, value));
  }

  private arrayEquals(a: unknown[], b: unknown[]): boolean {
    if (a.length !== b.length) return false;

    return a.every((item, index) => {
      const bItem = b[index];

      if (Array.isArray(item) && Array.isArray(bItem)) {
        return this.arrayEquals(item, bItem);
      }

      if (
        item && typeof item === "object" && bItem && typeof bItem === "object"
      ) {
        return JSON.stringify(item) === JSON.stringify(bItem);
      }

      return item === bItem;
    });
  }

  async insertMany(
    docs: (Omit<T, "_id"> & { _id?: ObjectId })[],
    options: InsertManyOptions = {},
  ): Promise<InsertManyResult> {
    if (!Array.isArray(docs)) {
      throw new Error("docs parameter must be an array");
    }

    const { ordered = true } = options;
    const insertedIds: ObjectId[] = [];
    const writeErrors: { index: number; error: Error }[] = [];

    // Validate documents first
    for (let i = 0; i < docs.length; i++) {
      const doc = docs[i];
      if (!doc || typeof doc !== "object") {
        const error = new Error("Invalid document");
        if (ordered) throw error;
        writeErrors.push({ index: i, error });
        continue;
      }
    }

    // Helper function to insert a single document
    const insertDoc = async (
      doc: Omit<T, "_id"> & { _id?: ObjectId },
      index: number,
    ): Promise<ObjectId | null> => {
      try {
        // Skip already invalid documents
        if (!doc || typeof doc !== "object") {
          return null;
        }

        const _id = doc._id || new ObjectId();
        const key = this.getKvKey(_id);

        // Check for existing document with same _id
        const existing = await this.kv.get(key);
        if (existing.value) {
          const error = new Error("Duplicate key error");
          if (ordered) throw error;
          writeErrors.push({ index, error });
          return null;
        }

        const docToInsert = {
          ...doc,
          _id: this.serializeObjectId(_id),
        };

        await this.checkIndexViolations(
          docToInsert as Omit<T, "_id"> & { _id: string },
        );

        const result = await this.kv.atomic()
          .check({ key, versionstamp: null })
          .set(key, docToInsert)
          .commit();

        if (!result.ok) {
          throw new Error("Failed to insert document");
        }

        return _id;
      } catch (error) {
        writeErrors.push({
          index,
          error: error instanceof Error ? error : new Error(String(error)),
        });
        if (ordered) throw error;
        return null;
      }
    };

    try {
      if (ordered) {
        for (let i = 0; i < docs.length; i++) {
          const _id = await insertDoc(docs[i], i);
          if (_id) insertedIds.push(_id);
        }
      } else {
        await Promise.all(
          docs.map(async (doc, i) => {
            const _id = await insertDoc(doc, i);
            if (_id) insertedIds.push(_id);
          }),
        );
      }
    } catch (error) {
      if (ordered) {
        throw error; // Re-throw in ordered mode
      }
    }

    return {
      insertedCount: insertedIds.length,
      insertedIds,
      hasWriteErrors: writeErrors.length > 0,
      writeErrors,
    };
  }

  private isComparable(value: unknown): value is number | string | Date {
    return (
      typeof value === "number" ||
      typeof value === "string" ||
      value instanceof Date
    );
  }

  private async updateIndexEntry(
    doc: Omit<T, "_id"> & { _id: ObjectId },
    indexSpec: IndexDefinition,
    options: IndexOptions,
  ): Promise<void> {
    const fields = Object.keys(indexSpec.key);

    for (const field of fields) {
      const value = this.getNestedValue(doc, field);
      const serializedValue = this.serializeIndexValue(value);

      // Create index key: [collection, __idx__, field, serializedValue, docId]
      const indexKey = [
        this.collectionName,
        "__idx__",
        field,
        serializedValue,
        doc._id.toString(),
      ] as const;

      // For unique indexes, check if value already exists
      if (options.unique) {
        const prefix = [
          this.collectionName,
          "__idx__",
          field,
          serializedValue,
        ] as const;

        for await (const entry of this.kv.list({ prefix })) {
          const existingId = (entry.value as any)._id;
          if (existingId && existingId !== doc._id.toString()) {
            throw new Error(`Duplicate key error: ${field}`);
          }
        }
      }

      // Add the index entry
      await this.kv.atomic()
        .set(indexKey, { _id: doc._id.toString() })
        .commit();
    }
  }

  /**
   * Creates an index on the specified field(s).
   *
   * @example
   * ```ts
   * // Create a simple index
   * await users.createIndex("email");
   *
   * // Create a unique index
   * await users.createIndex({ key: { email: 1 }, options: { unique: true } });
   *
   * // Create a compound index
   * await users.createIndex({ key: { age: -1, name: 1 } });
   * ```
   *
   * @param fieldOrSpec The field name or index specification
   * @param options Options for the index
   * @returns A promise that resolves to the name of the created index
   */
  async createIndex(
    fieldOrSpec: string | IndexDefinition,
    options: IndexOptions = {},
  ): Promise<string> {
    // Validate options first
    const validOptionKeys = ["unique", "sparse", "name"];
    const invalidOptions = Object.keys(options).filter((key) =>
      !validOptionKeys.includes(key)
    );
    if (invalidOptions.length > 0) {
      throw new Error("Invalid index options");
    }

    // Normalize the index specification
    const indexSpec = this.normalizeIndexSpec(fieldOrSpec);

    // Validate index specification
    if (!indexSpec.key || Object.keys(indexSpec.key).length === 0) {
      throw new Error("Invalid index specification");
    }

    // Generate index name if not provided
    const indexName = options.name || Object.entries(indexSpec.key)
      .map(([field, dir]) => `${field}_${dir}`).join("_");

    // Check if index already exists
    const indexKey = ["__indexes__", this.collectionName, indexName] as const;
    const existingIndex = await this.kv.get(indexKey);

    if (existingIndex.value) {
      throw new Error("Index already exists");
    }

    // Get all existing documents
    const documents: WithId<T>[] = [];
    for await (const entry of this.kv.list({ prefix: [this.collectionName] })) {
      if (entry.key.length === 2) { // Only get main documents, not index entries
        const doc = entry.value as WithId<T>;
        doc._id = this.deserializeObjectId(doc._id as unknown as string);
        documents.push(doc);
      }
    }

    // Check for uniqueness constraint violations before building index
    if (options.unique) {
      const valueMap = new Map<string, Set<string>>();

      for (const doc of documents) {
        for (const field of Object.keys(indexSpec.key)) {
          const value = this.getNestedValue(doc, field);
          const serializedValue = this.serializeIndexValue(value);
          const key = `${field}:${serializedValue}`;

          if (!valueMap.has(key)) {
            valueMap.set(key, new Set());
          }

          const docIds = valueMap.get(key)!;
          if (docIds.size > 0) {
            throw new Error(`Duplicate key error: ${field}`);
          }
          docIds.add(doc._id.toString());
        }
      }
    }

    // Store index metadata
    await this.kv.set(indexKey, { spec: indexSpec, options });

    // Build the index for existing documents
    for (const doc of documents) {
      await this.updateIndexEntry(doc, indexSpec as IndexDefinition, options);
    }

    return indexName;
  }

  private serializeIndexValue(value: unknown): string | number | boolean {
    if (value instanceof Date) {
      return value.toISOString();
    }
    if (value instanceof ObjectId) {
      return value.toString();
    }
    if (value === null || value === undefined) {
      return "";
    }
    if (
      typeof value === "string" || typeof value === "number" ||
      typeof value === "boolean"
    ) {
      return value;
    }
    return JSON.stringify(value);
  }

  private applyFindOptions(
    results: WithId<T>[],
    options: FindOptions<T>,
  ): WithId<T>[] {
    let processed = results;

    if (options.sort) {
      processed = this.sortDocuments(processed, options.sort);
    }

    processed = processed.slice(
      options.skip || 0,
      options.limit ? (options.skip || 0) + options.limit : undefined,
    );

    if (options.projection) {
      return processed.map((doc) =>
        this.applyProjection(doc, options.projection)
      );
    }

    return processed;
  }

  async listIndexes(): Promise<
    { name: string; spec: IndexDefinition; options: IndexOptions }[]
  > {
    const prefix = ["__indexes__", this.collectionName] as const;
    const indexes: {
      name: string;
      spec: IndexDefinition;
      options: IndexOptions;
    }[] = [];

    for await (const entry of this.kv.list({ prefix })) {
      const name = entry.key[2] as string;
      const { spec, options } = entry.value as IndexInfo;
      indexes.push({ name, spec, options });
    }

    return indexes;
  }

  async dropIndex(indexName: string): Promise<void> {
    const indexKey = ["__indexes__", this.collectionName, indexName] as const;
    await this.kv.delete(indexKey);
  }

  private normalizeIndexSpec(
    fieldOrSpec: string | IndexDefinition,
  ): IndexDefinition {
    // Normalize the index specification
    return typeof fieldOrSpec === "string"
      ? { key: { [fieldOrSpec]: 1 } }
      : fieldOrSpec;
  }
}

/**
 * The main database class that provides access to collections.
 *
 * @example
 * ```ts
 * const db = new Database(await Deno.openKv());
 * const users = db.collection<User>("users");
 * ```
 */
class Database {
  private kv: Deno.Kv;

  /**
   * Creates a new Database instance.
   *
   * @param kv The Deno.Kv instance to use for storage
   */
  constructor(kv: Deno.Kv) {
    this.kv = kv;
  }

  /**
   * Gets a collection with the specified name.
   *
   * @template T The document type stored in the collection
   * @param name The name of the collection
   * @returns A Collection instance for the specified name
   */
  collection<T extends Document>(name: string): Collection<T> {
    return new Collection<T>(this.kv, name);
  }
}

export { Collection, Database, ObjectId };
