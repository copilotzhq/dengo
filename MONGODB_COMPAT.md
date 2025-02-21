# MongoDB Compatibility Layer

This document tracks the compatibility between our Deno KV MongoDB-like API and the official MongoDB Node.js driver.

## Implemented Features

### Query Methods

#### collection.findOne(filter, options)
- [x] Filter support
  - Basic filters
  - Comparison operators
  - Logical operators
  - Array operators
  - Element operators
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

#### Comparison
- [x] `$eq` - Matches values that are equal to a specified value
- [x] `$gt` - Matches values that are greater than a specified value
- [x] `$gte` - Matches values that are greater than or equal to a specified value
- [x] `$lt` - Matches values that are less than a specified value
- [x] `$lte` - Matches values that are less than or equal to a specified value
- [x] `$in` - Matches any of the values specified in an array
- [x] `$ne` - Matches values that are not equal to a specified value
- [x] `$nin` - Matches none of the values specified in an array

#### Logical
- [x] `$and` - Joins query clauses with a logical AND
- [x] `$or` - Joins query clauses with a logical OR
- [x] `$not` - Inverts the effect of a query expression
- [x] `$nor` - Joins query clauses with a logical NOR

#### Array
- [x] `$all` - Matches arrays that contain all elements
- [x] `$elemMatch` - Matches documents that contain an array element matching all conditions
- [x] `$size` - Matches arrays with specific size

#### Element
- [x] `$exists`
  - Implemented with consistent ordering when multiple documents match
  - Always includes `_id` sort when no explicit sort is provided
  - For consistent results, combine with other filters to ensure unique matches
- [x] `$type` - Matches documents where field is of specified type
  - Supports both string types and MongoDB numeric BSON types
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
   - `backwards` - Backwards ordering

3. Index Hints
   - Named index hints
   - Index specification hints

4. Array Filters
   - Positional filtered updates
   - Array element matching

## Limitations and Differences from MongoDB

### Storage and Performance
1. No Indexes
   - No support for custom indexes
   - No index-based query optimization
   - Full collection scans for all queries
   - Limited performance on large collections

2. Transaction Limitations
   - Single-document atomicity only
   - No multi-document transactions
   - No distributed transactions
   - Limited isolation levels

3. Size Limitations
   - Document size limited by Deno KV value size limits
   - Collection size limited by Deno KV storage capacity
   - No automatic sharding or partitioning

### Query and Update Limitations
1. Query Operators
   - No regex support in queries
   - No geospatial operators
   - No text search capabilities
   - Limited `$type` operator functionality

2. Update Operators
   - No bitwise update operators
   - Limited array positional operator support
   - No JavaScript expression evaluation

3. Projection Limitations
   - No computed fields in projections
   - No `$` positional projection operator
   - No aggregation pipeline in projections

### Feature Differences
1. No Cursor API
   - All results returned as arrays
   - No streaming result support
   - Memory limitations for large result sets

2. No Change Streams
   - No real-time update notifications
   - No oplog or change tracking

3. Authentication and Authorization
   - No built-in user authentication
   - No role-based access control
   - Relies on Deno KV's security model

4. Data Types
   - Limited BSON type support
   - No support for MongoDB-specific types (Decimal128, etc.)
   - JavaScript Date used instead of MongoDB Date

### Administrative Features
1. No Administrative Commands
   - No database commands
   - No server statistics
   - No profiling tools

2. No Backup/Restore
   - No native dump/restore tools
   - Relies on Deno KV backup mechanisms

3. No Replication
   - No replica sets
   - No automatic failover
   - No secondary reads

### Development Considerations
1. Performance Optimization
   - Careful query design needed
   - Avoid large result sets
   - Consider data denormalization

2. Data Modeling
   - Prefer flatter document structures
   - Avoid deep nesting
   - Consider KV-oriented design patterns

3. Error Handling
   - Different error codes and messages
   - Simplified error classification
   - No support for write concerns
