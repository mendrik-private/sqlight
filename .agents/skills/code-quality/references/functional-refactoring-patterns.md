# Functional Programming Refactoring Patterns

Transform imperative, stateful code into functional, declarative code following modern FP best practices.

## Table of Contents

- [Core FP Principles](#core-fp-principles)
- [Transformation Patterns](#transformation-patterns)
- [FP-Specific Code Smells](#fp-specific-code-smells)
- [Benefits of Functional Refactoring](#benefits-of-functional-refactoring)
- [Finding Specific Content](#finding-specific-content)

---

## Core FP Principles

### Immutability
**Definition:** Data cannot be modified after creation; return new copies instead
**Benefits:** Predictable state, no hidden mutations, safe concurrency
**Application:** Use spread operators, Array.map/filter instead of mutations

### Pure Functions
**Definition:** Same input always produces same output; no side effects
**Benefits:** Deterministic behavior, easy testing, referential transparency
**Application:** No external state access, no I/O, no mutations

### Declarative Style
**Definition:** Express what to compute, not how
**Benefits:** Intent over implementation, less error-prone
**Application:** map/filter/reduce instead of for-loops

### Function Composition
**Definition:** Build complex operations from simple functions
**Benefits:** Reusability, modularity, pipeline thinking
**Application:** compose(...fns), pipe(...fns), chaining operations

### First-Class Functions
**Definition:** Functions as values - pass, return, store
**Benefits:** Higher-order functions, callbacks, strategies
**Application:** Functions as parameters, return functions, store in data structures

---

## Transformation Patterns

### 1. Replace Loops with Map/Filter/Reduce

**Before (Imperative):**
```javascript
const results = [];
for (let i = 0; i < items.length; i++) {
  if (items[i].active) {
    results.push(items[i].name.toUpperCase());
  }
}
```

**After (Functional):**
```javascript
const results = items
  .filter(item => item.active)
  .map(item => item.name.toUpperCase());
```

**Benefits:**
- No mutation of `results` array
- No index tracking
- Declarative intent (filter then transform)
- Chainable operations

**When to Apply:**
- Any for/while loop building collection
- Array processing with conditionals
- Multiple transformation steps

---

### 2. Extract Pure Functions

**Before (Impure - side effects):**
```javascript
let total = 0;
function addToTotal(value) {
  total += value;  // Mutates external state
  console.log('Adding:', value);  // Side effect: I/O
  return total;
}
```

**After (Pure):**
```javascript
function add(a, b) {
  return a + b;  // No side effects, deterministic
}

// Usage
const total = items.reduce(add, 0);
```

**Benefits:**
- Testable without mocks
- No hidden dependencies
- Safe to parallelize
- Referentially transparent

**When to Apply:**
- Functions accessing global state
- Functions with I/O mixed with logic
- Functions with unpredictable behavior

---

### 3. Higher-Order Functions (HOFs)

**Pattern:** Functions that take or return functions

**HOF Returning Function (Currying-like):**
```javascript
const multiplyBy = (factor) => (number) => number * factor;

// Specialized functions
const double = multiplyBy(2);
const triple = multiplyBy(3);

double(5);  // 10
triple(5);  // 15
```

**HOF Taking Function (Callback):**
```javascript
const applyOperation = (operation, value) => operation(value);

applyOperation(double, 10);  // 20
applyOperation(Math.sqrt, 16);  // 4
```

**Benefits:**
- Code reuse through parameterization
- Delayed execution, lazy evaluation
- Strategy pattern without classes

**When to Apply:**
- Common operations with varying behavior
- Configurable transformations
- Abstraction over algorithms

---

### 4. Function Composition

**Pattern:** Combine simple functions into complex ones

**Individual Functions:**
```javascript
const trim = (str) => str.trim();
const lowercase = (str) => str.toLowerCase();
const removeSpaces = (str) => str.replace(/\s+/g, '');
```

**Compose Utility (right-to-left):**
```javascript
const compose = (...fns) => (x) =>
  fns.reduceRight((v, f) => f(v), x);
```

**Pipe Utility (left-to-right):**
```javascript
const pipe = (...fns) => (x) =>
  fns.reduce((v, f) => f(v), x);
```

**Usage:**
```javascript
const normalize = pipe(trim, lowercase, removeSpaces);

normalize('  Hello World  ');  // "helloworld"
```

**Benefits:**
- Build complex from simple
- Reusable pipeline
- Point-free style (no intermediate variables)

**When to Apply:**
- Multi-step transformations
- Data pipelines
- Reducing nested function calls

---

### 5. Currying

**Pattern:** Transform multi-argument function into single-argument sequence

**Before (Multiple Arguments):**
```javascript
const add = (a, b, c) => a + b + c;
add(1, 2, 3);  // 6
```

**After (Curried):**
```javascript
const curriedAdd = (a) => (b) => (c) => a + b + c;

// Partial application
const add5 = curriedAdd(5);
const add5And10 = add5(10);
add5And10(3);  // 18

// Or directly
curriedAdd(5)(10)(3);  // 18
```

**Auto-Curry Utility:**
```javascript
const curry = (fn) => {
  return function curried(...args) {
    if (args.length >= fn.length) {
      return fn.apply(this, args);
    } else {
      return (...moreArgs) => curried(...args, ...moreArgs);
    }
  };
};

const add = curry((a, b, c) => a + b + c);
add(1)(2)(3);  // 6
add(1, 2)(3);  // 6
add(1)(2, 3);  // 6
```

**Benefits:**
- Partial application
- Reusable specialized functions
- Function factories

**When to Apply:**
- Functions with multiple parameters
- Creating specialized versions
- Building function pipelines

---

### 6. Eliminate Mutation

**Before (Mutation):**
```javascript
const user = { name: 'Alice', age: 30 };
user.age = 31;  // Mutates object

const numbers = [1, 2, 3];
numbers.push(4);  // Mutates array
```

**After (Immutable):**
```javascript
// Object update
const user = { name: 'Alice', age: 30 };
const updatedUser = { ...user, age: 31 };  // New object

// Array update
const numbers = [1, 2, 3];
const newNumbers = [...numbers, 4];  // New array

// Nested update
const state = {
  user: { name: 'Alice', preferences: { theme: 'light' } }
};
const newState = {
  ...state,
  user: {
    ...state.user,
    preferences: {
      ...state.user.preferences,
      theme: 'dark'
    }
  }
};
```

**Immutability Libraries:**
```javascript
// Immer (JavaScript)
import produce from 'immer';

const nextState = produce(state, draft => {
  draft.user.preferences.theme = 'dark';
});

// Immutable.js
import { Map } from 'immutable';

const map1 = Map({ a: 1, b: 2 });
const map2 = map1.set('b', 3);  // New map
```

**Benefits:**
- No unexpected mutations
- Time-travel debugging
- Easier reasoning about state

**When to Apply:**
- Any state updates
- Array/object transformations
- React/Redux state management

---

### 7. Replace Null with Maybe/Option Monad

**Before (Null Checks):**
```javascript
function getUserEmail(user) {
  if (user && user.profile && user.profile.email) {
    return user.profile.email;
  }
  return null;
}

// Usage
const email = getUserEmail(user);
if (email) {
  sendEmail(email);
}
```

**After (Maybe Monad):**
```javascript
const Maybe = (value) => ({
  map: (fn) => value != null ? Maybe(fn(value)) : Maybe(null),
  flatMap: (fn) => value != null ? fn(value) : Maybe(null),
  getOrElse: (defaultValue) => value != null ? value : defaultValue,
  isPresent: () => value != null
});

const getUserEmail = (user) =>
  Maybe(user)
    .map(u => u.profile)
    .map(p => p.email)
    .getOrElse('no-email@example.com');

// Usage
const email = getUserEmail(user);
// No null check needed, always has value
sendEmail(email);
```

**Functional Libraries:**
```javascript
// Folktale (JavaScript)
import { Maybe } from 'folktale/maybe';

const result = Maybe.fromNullable(user)
  .map(u => u.profile)
  .map(p => p.email)
  .getOrElse('default@example.com');
```

**Benefits:**
- No null pointer exceptions
- Explicit handling of absence
- Composable null checks

**When to Apply:**
- Nested null checks
- Optional values
- Chaining operations on potentially null values

---

### 8. Separate Side Effects from Pure Logic

**Before (Mixed):**
```javascript
function processOrder(order) {
  // Pure calculation
  const total = order.items.reduce((sum, item) =>
    sum + item.price, 0
  );

  // Side effects mixed in
  saveToDatabase(order);
  sendEmail(order.customer);
  console.log('Order processed:', order.id);

  return total;
}
```

**After (Separated):**
```javascript
// Pure logic
function calculateOrderTotal(order) {
  return order.items.reduce((sum, item) => sum + item.price, 0);
}

function prepareOrderSummary(order, total) {
  return {
    orderId: order.id,
    customer: order.customer,
    total,
    timestamp: new Date().toISOString()
  };
}

// Side effects isolated
function performOrderEffects(orderSummary) {
  return Promise.all([
    saveToDatabase(orderSummary),
    sendEmail(orderSummary.customer, orderSummary),
    logOrder(orderSummary)
  ]);
}

// Orchestration
async function processOrder(order) {
  const total = calculateOrderTotal(order);  // Pure
  const summary = prepareOrderSummary(order, total);  // Pure
  await performOrderEffects(summary);  // Effects at boundary
  return total;
}
```

**Benefits:**
- Pure logic easy to test
- Side effects explicit and controlled
- Effects pushed to boundaries

**When to Apply:**
- Functions mixing calculation and I/O
- Business logic with database calls
- Any function with side effects

---

## FP-Specific Code Smells

### Mutation
**Smell:** Modifying variables/objects in place
**Refactoring:** Use spread operators, map/filter, immutable libraries
**Example:** `user.age++` → `{ ...user, age: user.age + 1 }`

### Side Effects in Pure Functions
**Smell:** I/O, logging, state changes in computation
**Refactoring:** Separate pure logic from effects
**Example:** Extract console.log, database calls, API requests

### Imperative Loops
**Smell:** Using for/while instead of map/filter/reduce
**Refactoring:** Replace with functional array methods
**Example:** for-loop building array → .map()/.filter()

### Nested Conditionals
**Smell:** Deep if-else instead of pattern matching/guards
**Refactoring:** Guard clauses, early returns, Maybe monad
**Example:** Nested if-else → guard clauses with early returns

### Null/Undefined Checks
**Smell:** Manual null handling everywhere
**Refactoring:** Maybe/Option monad
**Example:** Multiple `if (x != null)` → Maybe(x).map(...)

### Shared Mutable State
**Smell:** Global variables accessed by multiple functions
**Refactoring:** Pass state as parameters, return new state
**Example:** Global counter → functional accumulator

### Object-Oriented Patterns in FP
**Smell:** Classes with methods instead of functions with data
**Refactoring:** Convert to functions operating on data structures
**Example:** `user.getName()` → `getName(user)`

---

## Benefits of Functional Refactoring

### Testability
- Pure functions easy to test (no mocks, no setup)
- Deterministic: same input always produces same output
- No hidden dependencies or state

### Predictability
- Referential transparency
- No action at a distance
- Easier to reason about code

### Parallelism
- No shared mutable state
- Safe concurrent execution
- Automatic parallelization possible

### Debugging
- No hidden state changes
- Time-travel debugging (immutability)
- Stack traces more meaningful

### Reusability
- Composable functions more reusable than methods
- Higher-order functions create function families
- Small, focused functions easy to combine

---

## Finding Specific Content

Search patterns for quick reference:

```bash
# Find transformation pattern
grep -i "map filter reduce" references/functional-refactoring-patterns.md

# Find monad information
grep -i "maybe monad" references/functional-refactoring-patterns.md

# Find immutability patterns
grep -i "immutab" references/functional-refactoring-patterns.md

# Find HOF examples
grep -i "higher-order" references/functional-refactoring-patterns.md
```

---

**Sources:**
- Functional programming best practices (2024-2025)
- Martin Fowler's functional refactoring patterns
- Industry guides on immutability and pure functions
