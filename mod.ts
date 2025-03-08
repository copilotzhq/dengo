import { ObjectId } from "bson";

type SortDirection = 1 | -1;

interface UpdateResult<T> {
  matchedCount: number;
  modifiedCount: number;
  upsertedId: ObjectId | null;
  upsertedCount: number;
  acknowledged: boolean;
  hasWriteErrors?: boolean;
  writeErrors?: { index: number; error: Error }[];
}

type KvKeyPart = string | number | Uint8Array;
type KvKey = readonly [string, KvKeyPart, ...KvKeyPart[]];

interface FindOptions {
  sort?: Record<string, 1 | -1>;
  limit?: number;
  skip?: number;
  projection?: Record<string, 0 | 1 | boolean>;
}

interface InsertManyOptions {
  ordered?: boolean;
}

interface InsertManyResult {
  insertedCount: number;
  insertedIds: ObjectId[];
  hasWriteErrors: boolean;
  writeErrors?: { index: number; error: Error }[];
}

interface InsertOneResult {
  acknowledged: boolean;
  insertedId: ObjectId;
}

interface DeleteOptions {
  // Future options like collation, hint can be added here
}

interface DeleteResult {
  acknowledged: boolean;
  deletedCount: number;
}

interface CountOptions {
  limit?: number;
  skip?: number;
  // Future options like hint, maxTimeMS can be added here
}

interface DistinctOptions {
  // Future options like maxTimeMS, collation can be added here
}

// Comparison operator types
type ComparisonOperator<T> = {
  $eq?: T;
  $gt?: T;
  $gte?: T;
  $lt?: T;
  $lte?: T;
  $ne?: T;
  $in?: T[];
  $nin?: T[];
};

// Logical operator types
type LogicalOperator<T> = {
  $and?: Filter<T>[];
  $or?: Filter<T>[];
  $nor?: Filter<T>[];
  $not?: Filter<T>;
};

// Array operator types
type ArrayOperator<T> = {
  $all?: T[];
  $elemMatch?: Filter<T>;
  $size?: number;
};

// Element operator types
type ElementOperator = {
  $exists?: boolean;
  $type?: string;
};

// Filter type
type Filter<T = any> =
  & {
    [P in keyof T & string]?:
      | T[P]
      | ComparisonOperator<T[P]>
      | ArrayOperator<T[P]>
      | ElementOperator;
  }
  & LogicalOperator<T>;

// Update the Document interface to use ObjectId
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

// UpdateOperator type
type UpdateOperator<T> = {
  $set?: Partial<T>;
  $unset?: Partial<Record<string, true>>;
  $inc?: Partial<Record<string, number>>;
  $mul?: Partial<Record<string, number>>;
  $min?: Partial<Record<string, number | Date>>;
  $max?: Partial<Record<string, number | Date>>;
  $push?: Partial<Record<string, unknown>>;
  $pull?: Partial<Record<string, unknown>>;
  $addToSet?: Partial<Record<string, unknown>>;
};

// Update options with type safety
interface UpdateOptions<T> {
  upsert?: boolean;
  arrayFilters?: ArrayFilter<T>[];
  ordered?: boolean;
}

// Array filter type
type ArrayFilter<T> = {
  [key: string]: Filter<T>;
};

// Add type for stored documents (with required _id)
type WithId<T> = T & { _id: ObjectId };

// Move these outside the class
interface IndexOptions {
  unique?: boolean;
  sparse?: boolean;
  name?: string;
}

interface IndexDefinition {
  key: Record<string, 1 | -1>;
  options?: IndexOptions;
}

interface IndexInfo {
  spec: IndexDefinition;
  options: IndexOptions;
}

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
    return [this.collectionName, id.id] as const;
  }

  private generateId(): ObjectId {
    return new ObjectId();
  }

  private serializeObjectId(id: ObjectId): string {
    return id.toString();
  }

  private deserializeObjectId(id: string | Uint8Array): ObjectId {
    return new ObjectId(typeof id === "string" ? id : id.toString());
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
            if (existingId !== doc._id) {
              throw new Error(`Duplicate key error: ${field}`);
            }
          }
        }
      }
    }
  }

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
      const indexInfo = entry.value as {
        spec: IndexDefinition;
        options: IndexOptions;
      };
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

  private setNestedValue(obj: any, path: string, value: unknown): void {
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
          "$elemMatch" in condition))
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

    const results = await this.find(filter, { limit: 1, ...options });
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

  // Modify the existing find method to use indexes
  async find(
    filter: Filter<T>,
    options: FindOptions<T> = {},
  ): Promise<WithId<T>[]> {
    // Check if we can use an index
    const usableIndex = await this.findUsableIndex(filter);

    if (usableIndex) {
      return this.findUsingIndex(usableIndex, filter, options);
    }

    // Fall back to full collection scan
    return this.findWithoutIndex(filter, options);
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

  private getNestedValue(obj: any, path: string): unknown {
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
  }

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

  async updateMany(
    filter: Filter<T>,
    update: UpdateOperator<T>,
    options: UpdateOptions<T> = {},
  ): Promise<UpdateResult<T>> {
    const docs = await this.find(filter);

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

  async deleteMany(filter: Filter<T>): Promise<DeleteResult> {
    const docs = await this.find(filter);

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
    const { limit, skip = 0 } = options;

    // If no filter and no options, use fast path with list prefix
    if (Object.keys(filter).length === 0 && !limit && !skip) {
      let count = 0;
      const prefix = [this.collectionName];

      for await (const entry of this.kv.list({ prefix })) {
        count++;
      }

      return count;
    }

    // Otherwise, use find to apply filter
    const docs = await this.find(filter);

    // Apply skip and limit to the count
    const startIndex = skip;
    const endIndex = limit
      ? Math.min(startIndex + limit, docs.length)
      : docs.length;

    return Math.max(0, endIndex - startIndex);
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
    if (!field) {
      throw new Error("Field parameter required");
    }

    // Get filtered documents
    const docs = await this.find(filter);

    // Extract values from the specified field
    const values = new Set<unknown>();

    for (const doc of docs) {
      const value = this.getNestedValue(doc, field);

      if (value === undefined) {
        continue;
      }

      if (Array.isArray(value)) {
        // For array fields, add each unique element
        value.forEach((item) => {
          if (item !== undefined) {
            values.add(this.normalizeValue(item));
          }
        });
      } else {
        values.add(this.normalizeValue(value));
      }
    }

    return Array.from(values);
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
          if (existingId !== doc._id.toString()) {
            throw new Error(`Duplicate key error: ${field}`);
          }
        }
      }

      // Store the index entry atomically
      await this.kv.atomic()
        .set(indexKey, { _id: doc._id.toString() })
        .commit();
    }
  }

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
    const indexSpec = typeof fieldOrSpec === "string"
      ? { key: { [fieldOrSpec]: 1 } }
      : fieldOrSpec;

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
    // Deduplicate by field values first
    const uniqueResults = Array.from(
      new Map(
        results.map((doc) => [
          JSON.stringify([doc.name, doc.email, doc.age, doc.status]),
          doc,
        ]),
      ).values(),
    );

    let processed = uniqueResults;

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
}

class Database {
  private kv: Deno.Kv;

  constructor(kv: Deno.Kv) {
    this.kv = kv;
  }

  collection<T extends Document>(name: string): Collection<T> {
    return new Collection<T>(this.kv, name);
  }
}

export { Collection, Database, ObjectId };
