# OOP Refactoring Catalog

Comprehensive catalog of Object-Oriented refactoring patterns from Martin Fowler's "Refactoring: Improving the Design of Existing Code" (2nd Edition, 2018).

## Table of Contents

- [Method-Level Refactorings](#method-level-refactorings)
- [Class-Level Refactorings](#class-level-refactorings)
- [Conditional Refactorings](#conditional-refactorings)
- [Data Refactorings](#data-refactorings)
- [General Refactorings](#general-refactorings)
- [SOLID Principles](#solid-principles)
- [Automated Tooling](#automated-tooling)

---

## Method-Level Refactorings

### Extract Method
**Problem:** Long method doing too much
**Solution:** Break into focused, named functions
**When:** Method > 20-30 lines, complex logic, duplicated code fragments

### Inline Method
**Problem:** Method body clearer than method name
**Solution:** Replace method call with body content
**When:** Over-abstraction, trivial delegation, preparing for Extract Method elsewhere

### Extract Variable
**Problem:** Complex expression hard to understand
**Solution:** Assign expression to well-named variable
**When:** Nested calculations, repeated expressions, unclear intent

### Inline Variable
**Problem:** Variable adds no clarity
**Solution:** Replace variable with expression directly
**When:** Simple assignment, used once, refactoring preparation

### Replace Temp with Query
**Problem:** Temporary variable storing calculation
**Solution:** Convert to method returning value
**When:** Calculation used multiple times, promotes reusability

### Split Temporary Variable
**Problem:** Variable assigned multiple times for different purposes
**Solution:** Create separate variable for each purpose
**When:** Variable has multiple responsibilities, confusing updates

---

## Class-Level Refactorings

### Extract Class
**Problem:** Class doing work of two classes
**Solution:** Create new class, move relevant fields/methods
**When:** Large class, subset of methods operating on subset of data

### Inline Class
**Problem:** Class doing too little
**Solution:** Merge class into another
**When:** Class lost responsibility through refactoring, needless abstraction

### Move Method
**Problem:** Method uses another class more than its own
**Solution:** Move method to class it uses most
**When:** Feature Envy smell, improving cohesion

### Move Field
**Problem:** Field used more by another class
**Solution:** Move field to class that uses it most
**When:** Data and behavior should be together

### Hide Delegate
**Problem:** Client calling delegate object through another
**Solution:** Create delegating method on server
**When:** Reducing coupling, encapsulation improvement

### Remove Middle Man
**Problem:** Class doing too much delegation
**Solution:** Client calls delegate directly
**When:** Class exists only to delegate

### Introduce Foreign Method
**Problem:** Need method on class you can't modify
**Solution:** Create method in client class with first parameter as server object
**When:** Library/framework limitation, temporary solution

### Introduce Local Extension
**Problem:** Need several foreign methods
**Solution:** Create subclass or wrapper with needed methods
**When:** Multiple foreign methods needed, more permanent solution

---

## Conditional Refactorings

### Decompose Conditional
**Problem:** Complex conditional with unclear intent
**Solution:** Extract condition and each branch into named methods
**When:** Long conditionals, nested if-else, unclear business logic

### Consolidate Conditional Expression
**Problem:** Multiple conditionals with same result
**Solution:** Combine into single conditional, extract to method
**When:** Multiple checks leading to same action

### Consolidate Duplicate Conditional Fragments
**Problem:** Same code in every branch
**Solution:** Move common code outside conditional
**When:** Duplicated pre/post processing in branches

### Replace Conditional with Polymorphism
**Problem:** Conditional choosing behavior based on type
**Solution:** Create subclasses with overridden methods
**When:** Type checking with switch/if-else, OOP preferred

### Replace Nested Conditional with Guard Clauses
**Problem:** Deep nesting making normal path unclear
**Solution:** Use early returns for special cases
**When:** Nested if-else, abnormal conditions mixed with normal flow

### Introduce Null Object
**Problem:** Repeated null checks
**Solution:** Create null object class with do-nothing behavior
**When:** Many null checks, default behavior for null case

### Introduce Assertion
**Problem:** Assumption not explicitly stated
**Solution:** Add assertion to make assumption explicit
**When:** Documenting preconditions, debugging complex logic

---

## Data Refactorings

### Replace Magic Numbers
**Problem:** Numeric literals with unclear meaning
**Solution:** Create named constant or enum
**When:** Numbers with business meaning, repeated literals

### Introduce Parameter Object
**Problem:** Related parameters always passed together
**Solution:** Group into object
**When:** Long parameter lists, data clumps, 3+ related parameters

### Remove Parameter
**Problem:** Parameter no longer used
**Solution:** Delete parameter
**When:** Unused parameters, changing requirements

### Preserve Whole Object
**Problem:** Extracting multiple values from object to pass
**Solution:** Pass entire object
**When:** Getting several values from object, reducing parameter list

### Replace Parameter with Query
**Problem:** Parameter value derivable from another
**Solution:** Remove parameter, get value via method call
**When:** Removing unnecessary parameters, improving encapsulation

### Replace Query with Parameter
**Problem:** Method accessing global/external state
**Solution:** Pass value as parameter
**When:** Removing dependencies, making pure function

### Replace Data Value with Object
**Problem:** Primitive value needs additional data/behavior
**Solution:** Convert to object
**When:** Primitive Obsession smell, value has validation rules

### Change Value to Reference
**Problem:** Many identical instances of value object
**Solution:** Convert to reference object with single instance
**When:** Memory optimization, centralized updates

### Change Reference to Value
**Problem:** Reference object small, immutable, awkward to manage
**Solution:** Convert to value object
**When:** Simplifying design, enabling immutability

---

## General Refactorings

### Remove Dead Code
**Problem:** Unused methods, variables, parameters
**Solution:** Delete unused code
**When:** Code coverage reveals unused paths, outdated features

### Separate Query from Modifier
**Problem:** Method both returns value and changes state
**Solution:** Split into query and command methods
**When:** Side effects in queries, Command-Query Separation principle

### Parameterize Method
**Problem:** Several methods doing similar things with different values
**Solution:** Single method with parameter for varying value
**When:** Duplicated logic with literal variations

### Replace Constructor with Factory Method
**Problem:** Constructor doing more than simple construction
**Solution:** Replace with factory method
**When:** Complex creation logic, type-based construction, polymorphic creation

### Encapsulate Field
**Problem:** Public field
**Solution:** Make private, provide accessors
**When:** Direct field access, adding validation/transformation

### Encapsulate Collection
**Problem:** Method returning collection reference
**Solution:** Return read-only view, provide add/remove methods
**When:** Preventing external modification, maintaining invariants

### Replace Type Code with Class
**Problem:** Numeric/string type code
**Solution:** Create class for type code
**When:** Type safety, type-specific behavior

### Replace Type Code with Subclasses
**Problem:** Type code affecting behavior
**Solution:** Create subclass for each type
**When:** Type-dependent behavior, polymorphism preferred

### Replace Subclass with Fields
**Problem:** Subclasses differing only in constant values
**Solution:** Replace with fields in superclass
**When:** Trivial subclasses, reducing class explosion

---

## SOLID Principles

### Single Responsibility Principle (SRP)
**Definition:** Class should have one reason to change
**Violations:** Large classes, Divergent Change smell
**Refactorings:** Extract Class, Extract Method, Move Method

### Open/Closed Principle (OCP)
**Definition:** Open for extension, closed for modification
**Violations:** Modifying existing code for new features
**Refactorings:** Replace Conditional with Polymorphism, Strategy Pattern, Template Method

### Liskov Substitution Principle (LSP)
**Definition:** Subtypes must be substitutable for base types
**Violations:** Refused Bequest smell, strengthened preconditions
**Refactorings:** Extract Interface, Replace Inheritance with Delegation

### Interface Segregation Principle (ISP)
**Definition:** Clients depend only on methods they use
**Violations:** Fat interfaces, clients implementing unused methods
**Refactorings:** Extract Interface, Interface splitting

### Dependency Inversion Principle (DIP)
**Definition:** Depend on abstractions, not concretions
**Violations:** Direct dependencies on concrete classes
**Refactorings:** Extract Interface, Dependency Injection, Inversion of Control

---

## Automated Tooling

### Static Analysis Tools

**SonarQube** - Comprehensive code quality platform (all languages)
- Detects code smells, bugs, security vulnerabilities
- Quality gates, technical debt tracking
- Integration: CI/CD pipelines, IDE plugins

**ESLint** - JavaScript/TypeScript linting
- Customizable rules, auto-fix capabilities
- Integration: VSCode, WebStorm, CI/CD

**Pylint/Ruff** - Python code analysis
- PEP 8 compliance, code smell detection
- Fast performance (Ruff), comprehensive checks (Pylint)

**RuboCop** - Ruby static analysis
- Style guide enforcement, auto-correction
- Customizable rules, Rails integration

**Checkstyle/PMD/SpotBugs** - Java quality tools
- Checkstyle: Style compliance
- PMD: Code smell detection
- SpotBugs: Bug pattern detection

### IDE Refactoring Support

**Visual Studio Code**
- Built-in: Extract Method, Rename Symbol, Move to File
- Extensions: Refactoring tools, language-specific support

**IntelliJ IDEA**
- Advanced refactoring automation (20+ refactorings)
- Safe refactoring with usage search
- Language support: Java, Kotlin, JavaScript, etc.

**Eclipse**
- Extensive refactoring tools
- Quick fixes, automated transformations

**PyCharm**
- Python-specific refactorings
- Type-aware transformations

### CI/CD Integration

**Quality Gates**
- Set thresholds: code coverage (>80%), complexity limits
- Fail builds on violations
- Prevent technical debt accumulation

**Continuous Monitoring**
- Track metrics over time
- Trend analysis, regression detection
- Dashboard reporting

---

## Finding Specific Content

Search patterns for quick reference:

```bash
# Find specific refactoring
grep -i "extract method" references/oop-refactoring-catalog.md

# Find SOLID principle
grep -i "single responsibility" references/oop-refactoring-catalog.md

# Find tooling information
grep -i "sonarqube" references/oop-refactoring-catalog.md
```

---

**Source:** Martin Fowler, "Refactoring: Improving the Design of Existing Code" (2nd Edition, 2018)
**Additional:** refactoring.guru, industry best practices
