import { ObjectId } from "npm:bson"

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
type Filter<T = any> = {
  [P in keyof T & string]?:
  | T[P]
  | ComparisonOperator<T[P]>
  | ArrayOperator<T[P]>
  | ElementOperator;
} & LogicalOperator<T>;

// Update the Document interface to use ObjectId
interface Document {
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
  [key: string]: Filter<T>
}

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

class Collection<T extends Omit<Document, '_id'> & { _id?: ObjectId }> {
  private kv: Deno.Kv;
  private collectionName: string;

  constructor(kv: Deno.Kv, collectionName: string) {  
    this.kv = kv;
    this.collectionName = collectionName;
    console.log('Collection initialized:', collectionName);
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
    return new ObjectId(typeof id === 'string' ? id : id.toString());
  }

  async insertOne(doc: T): Promise<InsertOneResult> {
    // Handle invalid document cases
    if (!doc || typeof doc !== 'object') {
      throw new Error("Invalid document");
    }

    // Generate or use existing _id
    const _id = doc._id || this.generateId();
    const key = this.getKvKey(_id);

    // Create document with _id, ensuring ObjectId is properly serialized
    const docToInsert = {
      ...doc,
      _id: this.serializeObjectId(_id)
    };

    // Check for existing document with same _id
    const existing = await this.kv.get(key);
    if (existing.value) {
      throw new Error("Duplicate key error");
    }

    // Validate document fields
    this.validateDocument(docToInsert);

    // Insert document atomically
    const result = await this.kv.atomic()
      .check({ key, versionstamp: null })
      .set(key, docToInsert)
      .commit();

    if (!result.ok) {
      throw new Error("Failed to insert document");
    }

    return {
      acknowledged: true,
      insertedId: _id // Return the original ObjectId
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

      if (typeof value === 'object' && value !== null) {
        Object.values(value).forEach(validateField);
        return;
      }

      // Basic types are always valid
      if (['string', 'number', 'boolean', 'undefined'].includes(typeof value)) {
        return;
      }

      // If we get here, we have an invalid type
      throw new Error(`Invalid field type: ${typeof value}`);
    };

    validateField(doc);
  }

  private applyProjection<D extends Document>(
    doc: D,
    projection?: Record<string, number | boolean>
  ): WithId<T> {
    if (!projection || Object.keys(projection).length === 0) {
      return doc as unknown as WithId<T>;
    }

    // Check for mixed inclusion/exclusion
    const values = Object.entries(projection)
      .filter(([key]) => key !== '_id')
      .map(([_, value]) => value);
    
    if (values.length > 0 && values.some(v => v === 1) && values.some(v => v === 0)) {
      throw new Error("Projection cannot have a mix of inclusion and exclusion");
    }

    const result: Record<string, unknown> = { _id: doc._id };
    const includeMode = values.some(v => v === 1 || v === true);

    if (includeMode) {
      // Include mode: only add specified fields
      Object.entries(projection).forEach(([path, value]) => {
        if (value === 1 || path === '_id') {
          const val = this.getNestedValue(doc, path);
          if (val !== undefined) {
            this.setNestedValue(result, path, val);
          }
        }
      });
    } else {
      // Exclude mode: copy all fields except excluded ones
      Object.entries(doc).forEach(([key, value]) => {
        if (projection[key] !== 0 && key !== '_id') {
          result[key] = value;
        }
      });
    }

    return result as WithId<T>;
  }

  private setNestedValue(obj: any, path: string, value: unknown): void {
    const parts = path.split('.');
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
    if (a instanceof ObjectId && b instanceof ObjectId) {
      return a.equals(b);
    }
    if (a === b) return true;
    if (typeof a !== 'object' || typeof b !== 'object') return false;
    if (a === null || b === null) return false;

    if (Array.isArray(a) && Array.isArray(b)) {
      return a.length === b.length &&
        a.every((item, index) => this.isEqual(item, b[index]));
    }

    const keysA = Object.keys(a as object);
    const keysB = Object.keys(b as object);

    if (keysA.length !== keysB.length) return false;

    return keysA.every(key =>
      Object.prototype.hasOwnProperty.call(b, key) &&
      this.isEqual((a as any)[key], (b as any)[key])
    );
  }

  private matchesCondition(value: unknown, condition: unknown): boolean {
    // Special handling for ObjectId
    if (value instanceof ObjectId && condition instanceof ObjectId) {
        return value.equals(condition);
    }

    // Handle array value matching
    if (Array.isArray(value) && !Array.isArray(condition) && 
        !(condition && typeof condition === 'object' && 
          ('$all' in condition || '$size' in condition))) {
        return value.some(item => this.matchesCondition(item, condition));
    }

    if (condition && typeof condition === 'object') {
        return Object.entries(condition as Record<string, unknown>).every(([op, val]) => {
            switch (op) {
                case '$eq': return this.isEqual(value, val);
                case '$gt': 
                    // Remove the hardcoded 'x' field handling
                    return this.isComparable(value) && this.isComparable(val) && value > val;
                case '$gte': return this.isComparable(value) && this.isComparable(val) && value >= val;
                case '$lt': return this.isComparable(value) && this.isComparable(val) && value < val;
                case '$lte': return this.isComparable(value) && this.isComparable(val) && value <= val;
                case '$ne': return !this.isEqual(value, val);
                case '$in': return Array.isArray(val) && val.some(v => this.isEqual(value, v));
                case '$nin': return Array.isArray(val) && !val.some(v => this.isEqual(value, v));
                case '$exists': return val ? value !== undefined : value === undefined;
                case '$all': return Array.isArray(value) && Array.isArray(val) &&
                    val.every(v => (value as unknown[]).some(item => this.isEqual(item, v)));
                case '$size': return Array.isArray(value) && value.length === val;
                default: return this.isEqual(value, condition);
            }
        });
    }

    // Direct value comparison
    return this.isEqual(value, condition);
  }

  private matchesFilter(doc: WithId<T>, filter: Filter<T>): boolean {
    return Object.entries(filter).every(([key, condition]) => {
        // Handle logical operators
        if (key === '$and' && Array.isArray(condition)) {
            return condition.every(subFilter => this.matchesFilter(doc, subFilter));
        }
        if (key === '$or' && Array.isArray(condition)) {
            return condition.some(subFilter => this.matchesFilter(doc, subFilter));
        }
        if (key === '$nor' && Array.isArray(condition)) {
            return !condition.some(subFilter => this.matchesFilter(doc, subFilter));
        }
        if (key === '$not') {
            return !this.matchesFilter(doc, condition as Filter<T>);
        }

        // Get value (handling nested paths)
        const value = key.includes('.') 
            ? this.getNestedValue(doc, key)
            : (key in doc ? doc[key] : undefined);
            
        // Add debug logging
        console.log('Matching:', { key, value, condition, doc });
        
        return this.matchesCondition(value, condition);
    });
  }

  async findOne(
    filter: Filter<T>,
    options: FindOptions<T> = {}
  ): Promise<WithId<T> | null> {
    if ('_id' in filter && filter._id instanceof ObjectId) {
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

  async find(
    filter: Filter<T>,
    options: FindOptions<T> = {}
  ): Promise<WithId<T>[]> {
    const { sort = { _id: 1 }, limit, skip = 0, projection } = options;
    let results: WithId<T>[] = [];

    // Add debug logging
    console.log('Find filter:', filter);

    for await (const entry of this.kv.list({ prefix: [this.collectionName] })) {
        // Deserialize the document's _id before matching
        const doc = entry.value as WithId<T>;
        doc._id = this.deserializeObjectId(doc._id as unknown as string);
        
        // Add debug logging
        console.log('Checking document:', doc);
        
        if (this.matchesFilter(doc, filter)) {
            console.log('Document matched:', doc);
            results.push(doc);
        }
    }

    // Sort results
    if (sort) {
        results = this.sortDocuments(results, sort);
    }

    // Apply skip and limit
    results = results.slice(skip, limit ? skip + limit : undefined);

    // Apply projection
    const projectedResults = results.map(doc => this.applyProjection(doc, projection));
    
    // Add debug logging
    console.log('Final results:', projectedResults);

    return projectedResults;
  }

  private sortDocuments(
    docs: WithId<T>[],
    sortOptions: Record<string, SortDirection>
  ): WithId<T>[] {
    const entries = Object.entries(sortOptions);

    return [...docs].sort((a, b) => {
      for (const [field, direction] of entries) {
        const aVal = this.getNestedValue(a, field);
        const bVal = this.getNestedValue(b, field);

        if (aVal === bVal) continue;

        // Handle ObjectId comparison
        if (aVal instanceof ObjectId && bVal instanceof ObjectId) {
          return direction * (aVal.equals(bVal) ? 0 : aVal.id > bVal.id ? 1 : -1);
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
    
    return path.split('.').reduce((current, part) => {
        if (current === undefined || current === null) return undefined;
        
        if (Array.isArray(current)) {
            // For array fields, map and filter
            const values = current.map(item => item[part]).filter(v => v !== undefined);
            return values.length ? values[0] : undefined;
        }
        
        // Handle nested object access
        return current && typeof current === 'object' ? current[part] : undefined;
    }, obj);
  }

  async updateOne(
    filter: Filter<T>,
    update: UpdateOperator<T>,
    options: UpdateOptions<T> = {}
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
        throw new Error('Failed to upsert document');
      }

      return {
        matchedCount: 0,
        modifiedCount: 1,
        upsertedId: _id,
        upsertedCount: 1,
        acknowledged: true
      };
    }

    if (!doc) {
      return {
        matchedCount: 0,
        modifiedCount: 0,
        upsertedId: null,
        upsertedCount: 0,
        acknowledged: true
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
      throw new Error('Failed to update document');
    }

    return {
      matchedCount: 1,
      modifiedCount: 1,
      upsertedId: null,
      upsertedCount: 0,
      acknowledged: true
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
      Object.keys(update.$unset).forEach(path => {
        const parts = path.split('.');
        let current = result;
        for (let i = 0; i < parts.length - 1; i++) {
          if (!current[parts[i]]) break;
          current = current[parts[i]] as any;
        }
        if (current && typeof current === 'object') {
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
          const filtered = array.filter(item => !this.isEqual(item, value));
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
    options: UpdateOptions<T> = {}
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
          _id: this.serializeObjectId(doc._id)
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
      writeErrors
    };
  }

  async deleteOne(filter: Filter<T>): Promise<DeleteResult> {
    const doc = await this.findOne(filter);

    if (!doc) {
      return {
        acknowledged: true,
        deletedCount: 0
      };
    }

    const key = this.getKvKey(doc._id);
    const currentEntry = await this.kv.get(key);

    const result = await this.kv.atomic()
      .check({ key, versionstamp: currentEntry.versionstamp })
      .delete(key)
      .commit();

    if (!result.ok) {
      throw new Error('Failed to delete document - concurrent modification detected');
    }

    return {
      acknowledged: true,
      deletedCount: 1
    };
  }

  async deleteMany(filter: Filter<T>): Promise<DeleteResult> {
    const docs = await this.find(filter);

    if (docs.length === 0) {
      return {
        acknowledged: true,
        deletedCount: 0
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
      throw new Error('Failed to delete documents - concurrent modification detected');
    }

    return {
      acknowledged: true,
      deletedCount: docs.length
    };
  }

  async countDocuments(
    filter: Filter<T> = {},
    options: CountOptions = {}
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
    const endIndex = limit ? Math.min(startIndex + limit, docs.length) : docs.length;

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
    options: DistinctOptions = {}
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
        value.forEach(item => {
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

    if (typeof value === 'object') {
      return JSON.stringify(value);
    }

    return value;
  }

  // Add this helper method for deep comparison
  private arrayIncludes(array: unknown[], value: unknown): boolean {
    return array.some(item => this.isEqual(item, value));
  }

  private arrayEquals(a: unknown[], b: unknown[]): boolean {
    if (a.length !== b.length) return false;

    return a.every((item, index) => {
      const bItem = b[index];

      if (Array.isArray(item) && Array.isArray(bItem)) {
        return this.arrayEquals(item, bItem);
      }

      if (item && typeof item === 'object' && bItem && typeof bItem === 'object') {
        return JSON.stringify(item) === JSON.stringify(bItem);
      }

      return item === bItem;
    });
  }

  async insertMany(
    docs: T[],
    options: InsertManyOptions = {}
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
      if (!doc || typeof doc !== 'object') {
        if (ordered) {
          throw new Error("Invalid document");
        }
        writeErrors.push({ 
          index: i, 
          error: new Error("Invalid document") 
        });
        continue;
      }
    }

    // Helper function to insert a single document
    const insertDoc = async (doc: T, index: number): Promise<ObjectId | null> => {
      try {
        const _id = doc._id || this.generateId();
        const key = this.getKvKey(_id);
        
        // Check for existing document
        const existing = await this.kv.get(key);
        if (existing.value) {
          throw new Error("Duplicate key error");
        }

        // Prepare document with serialized _id
        const docToInsert = {
          ...doc,
          _id: this.serializeObjectId(_id)
        };

        const result = await this.kv.atomic()
          .check({ key, versionstamp: null })
          .set(key, docToInsert)
          .commit();

        if (!result.ok) {
          throw new Error("Failed to insert document");
        }

        return _id;
      } catch (error) {
        if (ordered) {
          // In ordered mode, propagate the error
          throw error;
        }
        // In unordered mode, collect the error and continue
        writeErrors.push({
          index,
          error: error instanceof Error ? error : new Error(String(error))
        });
        return null;
      }
    };

    const validDocs = docs.filter((_, i) => !writeErrors.find(e => e.index === i));

    if (ordered) {
      // For ordered insertion, stop on first error
      try {
        for (const doc of validDocs) {
          const index = docs.indexOf(doc);
          const _id = await insertDoc(doc, index);
          if (_id) {
            insertedIds.push(_id);
          }
        }
      } catch (error) {
        // Propagate the error in ordered mode
        throw error;
      }
    } else {
      // For unordered insertion, try to insert all documents
      await Promise.all(
        validDocs.map(async (doc) => {
          const index = docs.indexOf(doc);
          const _id = await insertDoc(doc, index);
          if (_id) {
            insertedIds.push(_id);
          }
        })
      );
    }

    return {
      insertedCount: insertedIds.length,
      insertedIds,
      hasWriteErrors: writeErrors.length > 0,
      writeErrors
    };
  }

  private isComparable(value: unknown): value is number | string | Date {
    return typeof value === 'number' || typeof value === 'string' || value instanceof Date;
  }

  async createIndex(
    fieldOrSpec: string | IndexDefinition,
    options: IndexOptions = {}
  ): Promise<string> {
    const indexSpec = typeof fieldOrSpec === 'string' 
      ? { key: { [fieldOrSpec]: 1 } } 
      : fieldOrSpec;
    
    const indexName = options.name || Object.entries(indexSpec.key)
      .map(([field, dir]) => `${field}_${dir}`).join('_');

    // Store index metadata
    const indexKey = ["__indexes__", this.collectionName, indexName];
    await this.kv.set(indexKey, { spec: indexSpec, options });

    // Build the index
    for await (const entry of this.kv.list({ prefix: [this.collectionName] })) {
      const doc = entry.value as WithId<T>;
      await this.updateIndexEntry(doc, indexSpec as IndexDefinition, options);
    }

    return indexName;
  }

  private serializeIndexValue(value: unknown): string | number {
    if (value instanceof Date) {
      return value.getTime(); // Convert Date to timestamp for proper sorting
    }
    if (typeof value === 'string' || typeof value === 'number') {
      return value;
    }
    return JSON.stringify(value); // Fallback for other types
  }

  private async updateIndexEntry(
    doc: WithId<T>, 
    indexSpec: IndexDefinition,
    options: IndexOptions
  ): Promise<void> {
    const fields = Object.keys(indexSpec.key);
    
    for (const field of fields) {
      const value = this.getNestedValue(doc, field);
      if (value === undefined && options.sparse) continue;

      // Serialize the value for storage
      const serializedValue = this.serializeIndexValue(value);

      // Create index key: [collection, __idx__, field, serializedValue, docId]
      const indexKey = [
        this.collectionName,
        "__idx__",
        field,
        serializedValue,
        doc._id.toString()
      ];

      // Store the index entry with atomic operation
      if (options.unique) {
        // For unique indexes, check if value already exists
        const existing = await this.kv.get([
          this.collectionName,
          "__idx__",
          field,
          serializedValue
        ]);
        
        if (existing.value && (existing.value as any)._id !== doc._id.toString()) {
          throw new Error(`Duplicate key error: ${field}`);
        }
      }

      await this.kv.atomic()
        .set(indexKey, { _id: doc._id.toString() })
        .commit();
    }
  }

  private findDateRangeField(filter: Filter<T>): string | null {
    for (const [field, condition] of Object.entries(filter)) {
      if (condition && typeof condition === 'object') {
        const ops = Object.keys(condition as object);
        if (ops.some(op => ['$gt', '$gte', '$lt', '$lte'].includes(op))) {
          return field;
        }
      }
    }
    return null;
  }

  private async findUsingDateIndex(
    field: string,
    filter: Filter<T>,
    options: FindOptions<T>
  ): Promise<WithId<T>[]> {
    const condition = filter[field] as Record<string, Date>;
    const start = condition.$gt || condition.$gte;
    const end = condition.$lt || condition.$lte;

    const prefix = [this.collectionName, "__idx__", field] as const;
    
    // Create proper KvListSelector
    const selector: Deno.KvListSelector = end 
      ? { 
          start: [...prefix, this.serializeIndexValue(start)],
          end: [...prefix, this.serializeIndexValue(end)]
        }
      : { prefix };

    const results: WithId<T>[] = [];
    for await (const entry of this.kv.list(selector)) {
      const docId = (entry.value as any)._id;
      const doc = await this.findOne({ _id: new ObjectId(docId) } as Filter<T>);
      if (doc && this.matchesFilter(doc, filter)) {
        results.push(doc);
      }
    }

    return this.applyFindOptions(results, options);
  }

  private applyFindOptions(
    results: WithId<T>[],
    options: FindOptions<T>
  ): WithId<T>[] {
    const { sort, limit, skip = 0, projection } = options;

    let processed = results;
    
    if (sort) {
      processed = this.sortDocuments(processed, sort);
    }
    
    processed = processed.slice(skip, limit ? skip + limit : undefined);
    
    return processed.map(doc => this.applyProjection(doc, projection));
  }
}

class Database {
  private kv: Deno.Kv;

  constructor(kv: Deno.Kv) {
    this.kv = kv;
  }

  collection<T extends Document>(name: string) {
    return new Collection<T>(this.kv, name);
  }
}

export { ObjectId, Database, Collection };
