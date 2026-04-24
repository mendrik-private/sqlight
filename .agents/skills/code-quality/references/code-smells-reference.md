# Code Smells Reference

Comprehensive catalog of code smells organized by category, based on refactoring.guru and Martin Fowler's work.

## Table of Contents

- [Understanding Code Smells](#understanding-code-smells)
- [Category 1: Bloaters](#category-1-bloaters)
- [Category 2: Object-Orientation Abusers](#category-2-object-orientation-abusers)
- [Category 3: Change Preventers](#category-3-change-preventers)
- [Category 4: Dispensables](#category-4-dispensables)
- [Category 5: Couplers](#category-5-couplers)
- [Finding Specific Content](#finding-specific-content)

---

## Understanding Code Smells

**Definition:** Surface indications that usually correspond to deeper problems in the system. Not bugs—code works correctly—but weaknesses in design that slow development or increase risk of bugs.

**Origin:** Term coined by Kent Beck, popularized by Martin Fowler

**Key Principle:** Code smells are hints, not rules. Context matters. Use professional judgment.

---

## Category 1: Bloaters

Code, methods, and classes that have grown so large they're hard to work with. Usually accumulate over time as program evolves.

### Long Method (>20-30 lines)

**Description:** Method doing too much, hard to understand

**Signs:**
- Method exceeds 20-30 lines
- Multiple levels of abstraction
- Hard to name meaningfully
- Contains comments explaining sections

**Problems:**
- Cognitive overload
- Hard to test
- Difficult to reuse
- Hides business logic

**Refactorings:**
- Extract Method
- Replace Temp with Query
- Decompose Conditional
- Preserve Whole Object

**Example:**
```javascript
// Before: Long method
function processOrder(order) {
  // Validate order (10 lines)
  // Calculate totals (15 lines)
  // Apply discounts (20 lines)
  // Save to database (10 lines)
  // Send notifications (15 lines)
}

// After: Extracted methods
function processOrder(order) {
  validateOrder(order);
  const total = calculateTotal(order);
  const discountedTotal = applyDiscounts(total, order);
  saveOrder(order, discountedTotal);
  sendNotifications(order);
}
```

---

### Large Class (>200-300 lines)

**Description:** Class trying to do too much

**Signs:**
- Class exceeds 200-300 lines
- Many instance variables
- Many methods
- Violates Single Responsibility Principle

**Problems:**
- Hard to understand
- Difficult to maintain
- Low cohesion
- Changes affect many parts

**Refactorings:**
- Extract Class
- Extract Subclass
- Extract Interface
- Replace Data Value with Object

---

### Long Parameter List (>3-4 parameters)

**Description:** Too many parameters make method calls complex

**Signs:**
- Method takes >3-4 parameters
- Parameters often passed together
- Same parameters across multiple methods

**Problems:**
- Hard to remember parameter order
- Error-prone calls
- Difficult to read
- Often indicates missing abstraction

**Refactorings:**
- Introduce Parameter Object
- Preserve Whole Object
- Replace Parameter with Query

**Example:**
```javascript
// Before: Long parameter list
function createUser(name, email, age, address, phone, role, department) {
  // ...
}

// After: Parameter object
function createUser(userData) {
  // ...
}
```

---

### Primitive Obsession

**Description:** Overuse of primitives instead of small objects

**Signs:**
- Using strings/numbers for domain concepts
- Constants for coded information
- Field names simulating types (userID, userName)

**Problems:**
- Loss of type safety
- Validation scattered
- Business rules in multiple places
- Difficult to extend

**Refactorings:**
- Replace Data Value with Object
- Replace Type Code with Class
- Extract Class

**Example:**
```javascript
// Before: Primitive obsession
const email = "user@example.com";  // Just a string
function validateEmail(email) { /* validation */ }

// After: Value object
class Email {
  constructor(value) {
    if (!this.isValid(value)) {
      throw new Error('Invalid email');
    }
    this.value = value;
  }

  isValid(email) {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  }
}
```

---

### Data Clumps

**Description:** Same group of data items always appear together

**Signs:**
- Same 2-3 parameters in multiple methods
- Groups of fields that go together
- Removing one makes others meaningless

**Problems:**
- Duplicated parameter lists
- Missing abstraction
- Difficult to maintain

**Refactorings:**
- Extract Class
- Introduce Parameter Object
- Preserve Whole Object

---

## Category 2: Object-Orientation Abusers

Incomplete or incorrect application of object-oriented principles.

### Switch Statements

**Description:** Type checking with switch/if-else instead of polymorphism

**Signs:**
- Switch on type code
- Multiple places switching on same value
- Adding new type requires finding all switches

**Problems:**
- Violates Open/Closed Principle
- Scattered logic
- Easy to miss cases

**Refactorings:**
- Replace Conditional with Polymorphism
- Replace Type Code with Subclasses
- Introduce Null Object

**Example:**
```javascript
// Before: Switch statement
function getArea(shape) {
  switch (shape.type) {
    case 'circle':
      return Math.PI * shape.radius ** 2;
    case 'rectangle':
      return shape.width * shape.height;
    case 'triangle':
      return 0.5 * shape.base * shape.height;
  }
}

// After: Polymorphism
class Circle {
  getArea() { return Math.PI * this.radius ** 2; }
}
class Rectangle {
  getArea() { return this.width * this.height; }
}
class Triangle {
  getArea() { return 0.5 * this.base * this.height; }
}
```

---

### Temporary Field

**Description:** Field only set in certain circumstances

**Signs:**
- Instance variable used by only some methods
- Field null most of the time
- Confusing object state

**Problems:**
- Unexpected null values
- Hard to understand object lifecycle
- Maintenance confusion

**Refactorings:**
- Extract Class
- Introduce Null Object

---

### Refused Bequest

**Description:** Subclass doesn't use inherited methods

**Signs:**
- Subclass overrides methods to do nothing
- Subclass throws "not supported" errors
- Inheritance for code reuse, not specialization

**Problems:**
- Violates Liskov Substitution Principle
- Misleading hierarchy
- Fragile design

**Refactorings:**
- Replace Inheritance with Delegation
- Extract Superclass (push down used methods)

---

### Alternative Classes with Different Interfaces

**Description:** Classes doing similar things with different method names

**Signs:**
- Similar functionality, different interfaces
- Duplicated logic
- Should be interchangeable but aren't

**Problems:**
- Code duplication
- Can't use polymorphically
- Maintenance burden

**Refactorings:**
- Rename Method
- Move Method
- Extract Superclass

---

## Category 3: Change Preventers

Changes in one place require changes in many other places.

### Divergent Change

**Description:** Class changes for multiple reasons

**Signs:**
- One class modified for different types of changes
- "When we add X, we change these methods; when we add Y, we change those methods"
- Violates Single Responsibility Principle

**Problems:**
- High change frequency
- Risk of breaking unrelated functionality
- Hard to understand impact

**Refactorings:**
- Extract Class
- Extract Superclass
- Extract Subclass

**Example:**
```javascript
// Before: Divergent change
class User {
  // Database operations
  save() { /* ... */ }
  load() { /* ... */ }

  // Business logic
  validateEmail() { /* ... */ }
  hashPassword() { /* ... */ }

  // Presentation
  toJSON() { /* ... */ }
  toHTML() { /* ... */ }
}

// After: Separated concerns
class UserRepository {
  save(user) { /* ... */ }
  load(id) { /* ... */ }
}

class UserValidator {
  validateEmail(email) { /* ... */ }
}

class UserPresenter {
  toJSON(user) { /* ... */ }
  toHTML(user) { /* ... */ }
}
```

---

### Shotgun Surgery

**Description:** Single change requires many small edits across multiple classes

**Signs:**
- Adding feature requires changes in many places
- Changes scattered across codebase
- Hard to find all affected areas

**Problems:**
- Easy to miss changes
- High risk of bugs
- Time-consuming changes

**Refactorings:**
- Move Method
- Move Field
- Inline Class

---

### Parallel Inheritance Hierarchies

**Description:** Creating subclass in one hierarchy requires creating in another

**Signs:**
- Two hierarchies grow in parallel
- Adding to one means adding to other
- Often sharing prefix names

**Problems:**
- Duplicate work
- Easy to forget parallel change
- Maintenance burden

**Refactorings:**
- Move Method
- Move Field

---

## Category 4: Dispensables

Something pointless whose absence would make code cleaner and more efficient.

### Comments (Excessive)

**Description:** Comments explaining what code does (not why)

**Signs:**
- Comment explains what next lines do
- Comments compensate for poor naming
- Outdated comments

**Problems:**
- Comments lie (code changes, comments don't)
- Clutter
- Should be in code, not comments

**Refactorings:**
- Extract Method (name explains intent)
- Rename Method
- Introduce Assertion

**Note:** Good comments explain WHY, not WHAT

---

### Duplicate Code

**Description:** Same code structure in multiple places

**Signs:**
- Copy-pasted code
- Similar algorithms
- Same expressions in multiple methods

**Problems:**
- Changes needed in multiple places
- Inconsistent modifications
- Maintenance nightmare

**Refactorings:**
- Extract Method
- Extract Class
- Pull Up Method
- Form Template Method

---

### Dead Code

**Description:** Unused code

**Signs:**
- Unreachable code (after return)
- Unused methods, classes, variables
- Parameters never used

**Problems:**
- Maintenance confusion
- False sense of importance
- Wasted mental effort

**Refactorings:**
- Remove Dead Code (delete it!)

**How to Find:**
- Code coverage tools
- Static analysis
- IDE unused warnings

---

### Lazy Class

**Description:** Class doing too little to justify existence

**Signs:**
- Class with few methods
- Created for "future" that never came
- Simple delegations only

**Problems:**
- Unnecessary abstraction
- Maintenance overhead
- Clutters codebase

**Refactorings:**
- Inline Class
- Collapse Hierarchy

---

### Speculative Generality

**Description:** Unused abstraction for hypothetical future

**Signs:**
- Hooks and special cases "just in case"
- Abstract classes with one subclass
- Unused parameters
- Methods called from tests only

**Problems:**
- YAGNI violation
- Complexity without benefit
- Hard to understand purpose

**Refactorings:**
- Inline Class
- Remove Parameter
- Rename Method (make purpose clear)

---

## Category 5: Couplers

Excessive coupling between classes.

### Feature Envy

**Description:** Method uses another class more than its own

**Signs:**
- Method calling many getters on another object
- Logic clearly belongs elsewhere
- Method uses few of its own class's features

**Problems:**
- Wrong responsibility
- Poor cohesion
- Difficult to change

**Refactorings:**
- Move Method
- Extract Method then Move Method

**Example:**
```javascript
// Before: Feature envy
class OrderReport {
  generate(order) {
    // Uses order extensively
    const total = order.items.reduce((sum, item) =>
      sum + item.price * item.quantity, 0
    );
    const tax = total * order.taxRate;
    const shipping = order.calculateShipping();
    return total + tax + shipping;
  }
}

// After: Moved to Order
class Order {
  calculateTotal() {
    const subtotal = this.items.reduce((sum, item) =>
      sum + item.price * item.quantity, 0
    );
    const tax = subtotal * this.taxRate;
    const shipping = this.calculateShipping();
    return subtotal + tax + shipping;
  }
}
```

---

### Inappropriate Intimacy

**Description:** Classes too tightly coupled

**Signs:**
- Classes accessing each other's private fields
- Spending too much time together
- Changing one requires changing other

**Problems:**
- Hard to change independently
- Fragile design
- Testing difficulties

**Refactorings:**
- Move Method
- Move Field
- Extract Class
- Hide Delegate

---

### Message Chains

**Description:** Client asks object for another object, which asks for another...

**Signs:**
- Long chains: `a.getB().getC().getD()`
- Law of Demeter violations
- Client knows too much about structure

**Problems:**
- Fragile - breaks if chain changes
- High coupling
- Hard to test

**Refactorings:**
- Hide Delegate
- Extract Method

**Example:**
```javascript
// Before: Message chain
const manager = employee.getDepartment().getManager();

// After: Hide delegate
const manager = employee.getManager();

// In Employee class:
getManager() {
  return this.department.getManager();
}
```

---

### Middle Man

**Description:** Class exists only to delegate to another

**Signs:**
- Most methods just delegate
- Class adds no value
- Simple pass-through

**Problems:**
- Unnecessary indirection
- Maintenance overhead
- Confusing design

**Refactorings:**
- Remove Middle Man
- Inline Method
- Replace Delegation with Inheritance

---

### Incomplete Library Class

**Description:** Library missing needed functionality

**Signs:**
- Need methods library doesn't provide
- Can't modify library code
- Workarounds in multiple places

**Problems:**
- Scattered workarounds
- Duplicated solutions
- Difficult to change

**Refactorings:**
- Introduce Foreign Method
- Introduce Local Extension

---

## Finding Specific Content

Search patterns for quick reference:

```bash
# Find specific smell by name
grep -i "long method" references/code-smells-reference.md

# Find smell category
grep -i "bloaters" references/code-smells-reference.md

# Find refactoring for smell
grep -A 5 "Long Method" references/code-smells-reference.md | grep "Refactorings"

# Find all smells in category
grep -A 100 "Category 1: Bloaters" references/code-smells-reference.md
```

---

**Sources:**
- refactoring.guru - comprehensive code smell taxonomy
- Martin Fowler, "Refactoring" (2nd Edition, 2018)
- Industry best practices and patterns
