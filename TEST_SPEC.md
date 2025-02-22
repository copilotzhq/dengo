# Test Specification for MongoDB Compatibility Layer

This document outlines the test cases needed to verify the MongoDB-like API implementation.

## Query Methods Tests

### findOne
- [x] Basic document retrieval by _id
- [x] Retrieval with simple field equality
- [x] Comparison operators ($eq, $gt, $gte, $lt, $lte, $ne)
- [x] Logical operators ($and, $or, $not, $nor)
- [x] Array operators ($all, $elemMatch, $size)
- [x] Element operators ($exists, $type)
- [x] Projection tests
  - [x] Include specific fields
  - [x] Exclude specific fields
  - [x] Nested field projection
  - [x] Mixed projections (should fail)
  - [x] _id handling in projections
- [x] Sort options
- [x] Non-existent document handling

### find
- [x] Basic query with empty filter
- [x] Pagination (skip and limit)
- [x] Sorting (single field and multiple fields)
- [x] Complex filters with multiple operators
- [x] Array field queries
- [x] Nested document queries
- [x] Projection combinations
- [x] Edge cases (empty results, max limit)

### countDocuments
- [x] Count with empty filter
- [x] Count with simple equality filter
- [x] Count with complex query
- [x] Count with skip/limit
- [x] Count on empty collection
- [x] Count with array operators
- [x] Count with logical operators

### estimatedDocumentCount
- [x] Count on populated collection
- [x] Count on empty collection
- [x] Count accuracy verification

### distinct
- [x] Distinct on simple field
- [x] Distinct on nested field
- [x] Distinct with filter
- [x] Distinct on array field
- [x] Distinct with null/undefined values
- [x] Distinct on non-existent field
- [x] Distinct with complex filter

## Update Methods Tests

### updateOne
- [x] Basic field update ($set)
- [x] Numeric field operations ($inc, $mul)
- [x] Array operations ($push, $pull, $addToSet)
- [x] Nested field updates
- [x] Upsert behavior
- [x] Multiple operator combination
- [x] Update with empty filter
- [x] Update non-existent document
- [x] Field removal ($unset)
- [x] Array element updates
- [x] Min/Max operations

### updateMany
- [x] Update multiple documents
- [x] Array updates across documents
- [x] Complex filter matching
- [x] Empty result handling
- [x] Upsert with multiple matches
- [x] Partial update success handling
- [x] Transaction atomicity
- [x] Error handling

## Insert Methods Tests

### insertOne
- [x] Basic document insertion
- [x] Document with custom _id
- [x] Duplicate _id handling
- [x] Invalid document handling
- [x] Empty document handling
- [x] Large document handling
- [x] Special field types handling (null, boolean, number, Date, array, nested objects)

### insertMany
- [x] Multiple document insertion
- [x] Ordered vs unordered insertion
- [x] Duplicate key handling
- [x] Partial success handling
- [x] Empty array handling
- [x] Large batch handling
- [x] Error reporting accuracy

## Delete Methods Tests

### deleteOne
- [x] Delete by _id
- [x] Delete with complex filter
- [x] Delete non-existent document
- [x] Delete with empty filter
- [x] Verification of single deletion
- [x] Atomic operation verification

### deleteMany
- [x] Delete multiple documents
- [x] Delete with complex filter
- [x] Delete all documents
- [x] Delete non-matching filter
- [x] Transaction atomicity
- [x] Delete count accuracy

## Edge Cases and Error Handling

### Type Safety
- [x] Type validation in filters
- [x] Type validation in updates
- [x] Generic type enforcement
- [x] ObjectId type safety
- [x] Nested path type safety

## Index Management Tests

### createIndex
- [x] Basic index operations
  - [x] Create single-field index (ascending/descending)
  - [x] Create compound index
  - [x] Create index with options (unique, sparse)
  - [x] Create index on nested field

- [x] Index behavior verification
  - [x] Verify index improves query performance
  - [x] Test unique constraint enforcement
  - [x] Verify index updates on CRUD operations
  - [x] Test index with range queries

- [x] Error cases
  - [x] Duplicate index creation
  - [x] Invalid field/options
  - [x] Unique constraint violations

