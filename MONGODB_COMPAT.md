# MongoDB Compatibility Layer

This document tracks the compatibility between our Deno KV MongoDB-like API and the official MongoDB Node.js driver.

## Implemented Features

### Indexes

#### Supported Features
- [x] Single field indexes
  - [x] Ascending/descending order
  - [x] Unique constraint
  - [x] Range queries
  - [x] Sort operations
- [x] Compound indexes
  - [x] Multiple fields
  - [x] First field exact match
  - [x] Range queries on non-first fields
- [x] Index Options
  - [x] Unique indexes
  - [x] Sparse indexes
  - [x] Custom index names

#### Limitations
1. Performance
   - [x] Basic index-based query optimization implemented
   - [ ] No index intersection support
   - [ ] No index statistics or usage analysis
   - [ ] No automatic index cleanup on collection drop
   - [ ] No index size monitoring

2. Unsupported Index Types
   - [ ] Text indexes
   - [ ] Geospatial indexes
   - [ ] Hashed indexes
   - [ ] Wildcard indexes
   - [ ] Partial indexes
   - [ ] TTL indexes

3. Query Restrictions
   - [x] Single field indexes support range queries
   - [x] Compound indexes support exact + range queries
   - [ ] No index hints support
   - [ ] Limited to Deno KV's lexicographical ordering
   - [ ] No covered queries (always fetches full document)

4. Behavioral Differences
   - Indexes are stored in the same KV namespace using prefix "__idx__"
   - Different performance characteristics from MongoDB:
     - Range queries require serialization for proper ordering
     - No in-memory index structures
     - Sequential scans for non-indexed queries
   - No background index builds
   - No concurrent index creation

### Query Methods

#### collection.findOne(filter, options)
- [x] Filter support
  - Basic filters
  - Comparison operators
  - Logical operators
  - Array operators
  - Element operators
  - Nested field queries with dot notation
- [x] Options
  - [x] `projection` - Field inclusion/exclusion
    - Supports inclusion mode (`{ field: 1 }`)
    - Supports exclusion mode (`{ field: 0 }`)
    - Supports nested fields
    - Special `_id` handling
    - Prevents mixing include/exclude
  - [x] `sort` - Result ordering
    - Supports multiple fields
    - Ascending (1) and descending (-1)
- [ ] To Do
  - [ ] `collation` - Language-specific string comparison
  - [ ] `hint` - Index hint for query optimization
  - [ ] `comment` - Query comment for logging

#### collection.find(filter, options)
- [x] Filter support (same as findOne)
- [x] Options
  - [x] `limit` - Maximum number of documents
  - [x] `skip` - Number of documents to skip
  - [x] `sort` - Result ordering (1 ascending, -1 descending)
  - [x] `projection` - Field inclusion/exclusion
    - Supports inclusion mode (`{ field: 1 }`)
    - Supports exclusion mode (`{ field: 0 }`)
    - Supports nested fields
    - Special `_id` handling
    - Prevents mixing include/exclude
- [ ] To Do
  - [ ] `collation` - Language-specific string comparison
  - [ ] `hint` - Index hint
  - [ ] `batchSize` - Number of documents per batch
  - [ ] `comment` - Query comment
  - [ ] `maxTimeMS` - Maximum execution time

#### collection.countDocuments(filter, options)
- [x] Filter support
  - Basic filters
  - Comparison operators
  - Logical operators
  - Array operators
  - Element operators
- [x] Options
  - [x] `limit` - Maximum number of documents to count
  - [x] `skip` - Number of documents to skip
- [x] Optimizations
  - Fast path for unfiltered counts
  - Minimal field projection
  - Memory-efficient counting
- [ ] To Do
  - [ ] `hint` - Index hint for query optimization
  - [ ] `maxTimeMS` - Maximum execution time
  - [ ] `collation` - Language-specific string comparison

#### collection.estimatedDocumentCount()
- [x] Fast collection count
  - No filter support
  - Uses Deno KV list operation
  - Returns approximate count
- [ ] To Do
  - [ ] `maxTimeMS` - Maximum execution time

#### collection.distinct(field, filter, options)
- [x] Field support
  - Simple field values
  - Nested field paths (dot notation)
  - Array field elements
  - Object field values
- [x] Filter support
  - All query operators
  - Complex filters
  - Empty filters
- [x] Value handling
  - Unique value detection
  - Array element flattening
  - Null/undefined handling
  - Object comparison
- [ ] To Do
  - [ ] `maxTimeMS` - Maximum execution time
  - [ ] `collation` - Language-specific string comparison

### Update Methods

#### collection.updateOne(filter, update, options)
- [x] Filter support (same as findOne)
- [x] Update operators
  - All field update operators
  - All array update operators
- [x] Options
  - [x] `upsert` - Insert if no documents match
- [ ] To Do
  - [ ] `writeConcern` - Write concern level
  - [ ] `bypassDocumentValidation` - Skip document validation
  - [ ] `collation` - Language-specific string comparison
  - [ ] `arrayFilters` - Array update filters
  - [ ] `hint` - Index hint

#### collection.updateMany(filter, update, options)
- [x] Filter support (same as findOne)
- [x] Update operators (same as updateOne)
- [x] Options
  - [x] `upsert` - Insert if no documents match
- [ ] Unsupported Options
  - [ ] `writeConcern` - Write concern level
  - [ ] `bypassDocumentValidation` - Skip document validation
  - [ ] `collation` - Language-specific string comparison
  - [ ] `arrayFilters` - Array update filters
  - [ ] `hint` - Index hint

### Insert Methods

#### collection.insertOne(document, options)
- [x] Document insertion
  - Automatic _id generation
  - Basic field validation
- [ ] Options
  - [ ] `writeConcern` - Write concern level
  - [ ] `bypassDocumentValidation` - Skip document validation
  - [ ] `comment` - Operation comment

#### collection.insertMany(documents, options)
- [x] Bulk insert support
  - Automatic _id generation
  - Duplicate _id checking
  - Returns { acknowledged, insertedCount, insertedIds }
- [x] Options
  - [x] `ordered` - Whether to stop on first error
    - true: stops on first error (default)
    - false: continues on error, reports all errors
- [x] Error Handling
  - Duplicate key errors (code 11000)
  - Atomic operation failures
  - Bulk write errors with details
    - Error index
    - Error code
    - Error message
    - Failed operation
- [ ] Unsupported Options
  - [ ] `writeConcern` - Write concern level
  - [ ] `bypassDocumentValidation` - Skip document validation
  - [ ] `comment` - Operation comment

### Query Operators

#### Comparison Operators
- [x] `$eq` - Equal to
- [x] `$gt` - Greater than
- [x] `$gte` - Greater than or equal to
- [x] `$lt` - Less than
- [x] `$lte` - Less than or equal to
- [x] `$in` - In array
  - **Note**: When using `$in` with multiple ObjectIds, make sure to use the `toString()` method for comparison
- [x] `$ne` - Not equal to
- [x] `$nin` - Not in array

#### Logical
- [x] `$and` - Joins query clauses with a logical AND
  - Improved implementation with proper array handling
  - Supports nested conditions and complex filters
- [x] `$or` - Joins query clauses with a logical OR
  - Enhanced to properly handle array fields and nested objects
  - Supports complex combinations of filters
- [x] `$not` - Inverts the effect of a query expression
  - Full support for negating any query expression
- [x] `$nor` - Joins query clauses with a logical NOR
  - Complete implementation with proper array handling

#### Array
- [x] `$all` - Matches arrays that contain all elements
  - Improved implementation for better performance
  - Correctly handles nested arrays and complex objects
- [x] `$elemMatch` - Matches documents that contain an array element matching all conditions
  - Enhanced to support complex nested conditions
  - Works with all comparison and logical operators
- [x] `$size` - Matches arrays with specific size
  - Accurate array length matching

#### Element
- [x] `$exists`
  - Implemented with consistent ordering when multiple documents match
  - Always includes `_id` sort when no explicit sort is provided
  - For consistent results, combine with other filters to ensure unique matches
  - Improved handling of nested fields and arrays
- [x] `$type` - Matches documents where field is of specified type
  - Supports both string types and MongoDB numeric BSON types
  - Enhanced type detection for arrays, dates, and ObjectIds
  - Limited BSON type support compared to MongoDB

### Update Operators

#### Field Update Operators
- [x] `$set` - Sets the value of a field
- [x] `$setOnInsert` - Sets the value of a field only on upsert
- [x] `$inc` - Increments the value of a numeric field
- [x] `$mul` - Multiplies the value of a numeric field
- [x] `$min` - Updates field if specified value is less than existing
- [x] `$max` - Updates field if specified value is greater than existing
- [x] `$unset` - Removes specified fields
- [x] `$rename` - Renames a field

#### Array Update Operators
- [x] `$push` - Adds elements to an array
  - Supports `$each` for multiple elements
  - Supports `$slice` to limit array size
  - Supports `$sort` to sort array
  - Supports `$position` for insertion position
- [x] `$pull` - Removes elements from an array that match a condition
- [x] `$pullAll` - Removes all matching values from array
- [x] `$pop` - Removes first or last element from array
- [x] `$addToSet` - Adds elements to an array only if they don't exist

### Delete Methods

#### collection.deleteOne(filter, options)
- [x] Filter support
  - Basic filters
  - Comparison operators
  - Logical operators
  - Array operators
  - Element operators
- [x] Result object
  - `acknowledged` - Operation status
  - `deletedCount` - Number of deleted documents (0 or 1)
- [x] Atomic deletion
  - Single document deletion
  - Optimistic concurrency control
- [ ] Unsupported Options
  - [ ] `writeConcern` - Write concern level
  - [ ] `collation` - Language-specific string comparison
  - [ ] `hint` - Index hint for query optimization
  - [ ] `comment` - Operation comment

#### collection.deleteMany(filter, options)
- [x] Filter support (same as deleteOne)
- [x] Result object
  - `acknowledged` - Operation status
  - `deletedCount` - Number of deleted documents
- [x] Atomic deletion
  - Multiple document deletion in single transaction
  - Optimistic concurrency control
- [x] Complex filter support
  - Comparison operators
  - Logical operators
  - Array operators
  - Element operators
- [x] Transaction guarantees
  - All-or-nothing deletion
  - Version checking for each document
  - Concurrent modification protection
- [x] Error handling
  - Proper error reporting
  - Concurrent modification errors
  - Transaction failures
- [ ] Unsupported Options
  - [ ] `writeConcern` - Write concern level
  - [ ] `collation` - Language-specific string comparison
  - [ ] `hint` - Index hint for query optimization
  - [ ] `comment` - Operation comment

### Common Options Not Supported
1. Write Concerns
   - `w` - Write concern level
   - `j` - Journal write concern
   - `wtimeout` - Write concern timeout

2. Collation Options
   - `locale` - Locale for string comparison
   - `caseLevel` - Case sensitivity
   - `caseFirst` - Uppercase/lowercase ordering
   - `strength` - Comparison strength
   - `numericOrdering` - Numeric ordering
   - `alternate` - Alternate handling
   - `maxVariable` - Max variable
   - `backwards`

### Nested Field and Array Handling

#### Nested Field Access
- [x] Dot notation for nested fields
  - Supports deep nesting (e.g., `"user.address.city"`)
  - Works with all query operators
  - Handles missing intermediate fields gracefully

#### Array Field Queries
- [x] Direct array element matching
  - Matches if any array element equals the query value
  - Works with primitive values and objects
- [x] Array element position queries
  - Supports positional access (e.g., `"array.0"`)
  - Works with nested arrays
- [x] Array of objects queries
  - Supports querying by object fields (e.g., `"items.name"`)
  - Matches if any array element has matching field

#### Array Comparison Semantics
- [x] Array equality
  - Exact array matching (length and element order)
  - Element-by-element comparison
- [x] Array contains
  - Matches if array contains specified element
  - Works with all comparison operators
- [x] Array operator combinations
  - Supports complex queries with multiple array operators
  - Proper handling of nested arrays and objects