# Testing Strategies for Refactoring

Comprehensive guide to testing approaches that enable safe refactoring with behavior preservation.

## Table of Contents

- [Testing Philosophy](#testing-philosophy)
- [Characterization Tests](#characterization-tests)
- [Test-Driven Refactoring](#test-driven-refactoring)
- [Snapshot Testing](#snapshot-testing)
- [Test Coverage Strategies](#test-coverage-strategies)
- [Regression Prevention](#regression-prevention)
- [Finding Specific Content](#finding-specific-content)

---

## Testing Philosophy

**Core Principle:** Tests are your safety net for refactoring

**Key Concepts:**
- Tests document current behavior
- Fast, reliable tests enable confident changes
- Refactoring should never change observable behavior
- Tests run continuously during refactoring

**Testing Pyramid for Refactoring:**
```
        /\
       /E2E\       <- Few: Critical user journeys
      /------\
     /Integr.\    <- Some: Component interactions
    /----------\
   /Unit Tests \  <- Many: Pure functions, logic
  /--------------\
```

---

## Characterization Tests

### What Are Characterization Tests?

**Definition:** Tests that capture what code currently DOES, not what it SHOULD do

**Purpose:** Create safety net for legacy code without existing tests

**Key Principle:** "I don't know what this code should do, but I know what it does today"

### When to Use

- Legacy code without tests
- Undocumented behavior
- Before refactoring unfamiliar code
- Complex algorithms to preserve
- Code with unclear requirements

### How to Create

**1. Identify Behavior to Preserve:**
```javascript
// Legacy function - what does it do?
function processPayment(amount, type, customer) {
  // ... 50 lines of complex logic ...
  return result;
}
```

**2. Write Tests Capturing Current Behavior:**
```javascript
describe('processPayment - Characterization Tests', () => {
  test('basic credit card payment', () => {
    const result = processPayment(100, 'credit', { id: 1, tier: 'gold' });
    // Record what it ACTUALLY returns
    expect(result).toEqual({
      status: 'approved',
      amount: 100,
      fee: 2.9,
      total: 102.9
    });
  });

  test('gold tier gets discount', () => {
    const result = processPayment(100, 'credit', { id: 1, tier: 'gold' });
    expect(result.fee).toBe(2.4);  // Discovered behavior
  });

  test('handles negative amounts', () => {
    const result = processPayment(-50, 'credit', { id: 1 });
    // Even if wrong, capture current behavior
    expect(result.status).toBe('error');
  });
});
```

**3. Run Tests to Confirm Current Behavior:**
- Tests should pass immediately (describe reality)
- Adjust expectations if tests fail (discovering actual behavior)
- Don't fix bugs yet - just document them

**4. Now Safe to Refactor:**
- Tests prove behavior unchanged
- Can improve structure without fear
- Fix bugs in separate, explicit change

### Characterization Testing Tools

**Jest (JavaScript/TypeScript):**
```javascript
// Generate tests from execution
test('characterize getUserDetails', () => {
  const result = getUserDetails(123);
  expect(result).toMatchSnapshot();
});
```

**ApprovalTests (Multiple Languages):**
```python
from approvaltests import verify

def test_process_order():
    result = process_order(sample_order)
    verify(result)  # Stores result as approved file
```

**Example-Based Testing:**
```javascript
// Record inputs and outputs
const characterizationSuite = [
  { input: [100, 'credit'], output: { status: 'approved', amount: 100 } },
  { input: [0, 'debit'], output: { status: 'rejected', reason: 'invalid' } },
  // ... more cases discovered through exploration
];

characterizationSuite.forEach(({ input, output }) => {
  test(`handles ${JSON.stringify(input)}`, () => {
    expect(legacyFunction(...input)).toEqual(output);
  });
});
```

### Best Practices

✅ **Do:**
- Test actual behavior, not desired behavior
- Capture edge cases and corner cases
- Include "wrong" behavior (to be fixed later)
- Make tests comprehensive before refactoring
- Run tests frequently during refactoring

❌ **Don't:**
- Fix bugs while creating tests
- Assume behavior - verify it
- Write tests for how code should work
- Skip edge cases

---

## Test-Driven Refactoring

### Red-Green-Refactor Cycle

**Core TDD Loop:**
```
1. RED: Write failing test
    ↓
2. GREEN: Make it pass (quickly)
    ↓
3. REFACTOR: Improve design
    ↓
   (Repeat)
```

### Applying to Refactoring

**When You Have Tests:**
1. Ensure tests pass (GREEN)
2. Refactor code
3. Ensure tests still pass (stay GREEN)
4. Commit

**When Adding Features:**
1. RED: Write test for new feature
2. GREEN: Implement minimally
3. REFACTOR: Improve design
4. Commit

### Example: TDD Refactoring Session

**Starting Point:**
```javascript
// Ugly but working code
function calc(a, b, op) {
  if (op == '+') return a + b;
  if (op == '-') return a - b;
  if (op == '*') return a * b;
  if (op == '/') return a / b;
}

// Tests pass
test('calculator works', () => {
  expect(calc(5, 3, '+')).toBe(8);
  expect(calc(5, 3, '-')).toBe(2);
  expect(calc(5, 3, '*')).toBe(15);
  expect(calc(5, 3, '/')).toBe(1.666...);
});
```

**Refactoring Steps (staying green):**

**Step 1: Extract strategy map**
```javascript
const operations = {
  '+': (a, b) => a + b,
  '-': (a, b) => a - b,
  '*': (a, b) => a * b,
  '/': (a, b) => a / b
};

function calc(a, b, op) {
  return operations[op](a, b);
}

// Run tests: ✓ Still green
```

**Step 2: Add validation**
```javascript
function calc(a, b, op) {
  if (!operations[op]) {
    throw new Error(`Unknown operation: ${op}`);
  }
  return operations[op](a, b);
}

// Add test for new behavior (RED)
test('throws on invalid operation', () => {
  expect(() => calc(5, 3, '%')).toThrow('Unknown operation');
});

// Run tests: ✓ Green again
```

### TDD Best Practices for Refactoring

**Keep Tests Passing:**
- Make one small change
- Run tests
- If red, revert or fix immediately
- Never accumulate failing tests

**Test Behavior, Not Implementation:**
```javascript
// ❌ Bad: Tests implementation
test('uses addition function', () => {
  expect(calc.operations['+']).toBeDefined();
});

// ✅ Good: Tests behavior
test('adds numbers', () => {
  expect(calc(2, 3, '+')).toBe(5);
});
```

**Commit Frequently:**
- After each green refactoring
- Small, reversible steps
- Git history shows refactoring journey

---

## Snapshot Testing

### What is Snapshot Testing?

**Definition:** Capture output once, compare against it in future runs

**Use Cases:**
- Complex output structures
- UI components
- Data transformations
- API responses

### How It Works

**First Run:**
```javascript
test('renders user profile', () => {
  const output = renderProfile({ name: 'Alice', age: 30 });
  expect(output).toMatchSnapshot();
});

// Creates __snapshots__/test.spec.js.snap:
exports[`renders user profile 1`] = `
"<div class='profile'>
  <h1>Alice</h1>
  <p>Age: 30</p>
</div>"
`;
```

**Subsequent Runs:**
- Compares output to stored snapshot
- Fails if output differs
- Developer reviews: intentional change or bug?

### Best Practices

**✅ Good Uses:**
- Complex data structures
- React/Vue component output
- Generated HTML/XML
- Configuration objects

**❌ Avoid:**
- Data with timestamps/random values
- Tests that should be more specific
- Excessively large snapshots
- Binary or opaque data

**Handling Changes:**
```bash
# Review changes
npm test -- -u  # Update all snapshots

# Or selectively in test runner
# i - Update this snapshot
# u - Update all failing snapshots
# s - Skip this test
```

---

## Test Coverage Strategies

### Coverage Metrics

**Types of Coverage:**
- **Line Coverage**: % of lines executed
- **Branch Coverage**: % of if/else paths taken
- **Function Coverage**: % of functions called
- **Statement Coverage**: % of statements executed

**Target:** >80% for code being refactored

### Measuring Coverage

**Jest:**
```bash
npm test -- --coverage

# Output:
File      | % Stmts | % Branch | % Funcs | % Lines
----------|---------|----------|---------|--------
utils.js  |   85.71 |    75.00 |  100.00 |   85.71
```

**Istanbul (nyc):**
```bash
nyc mocha tests/

# HTML report
nyc --reporter=html mocha tests/
open coverage/index.html
```

### Prioritizing Coverage

**1. High-Risk Areas First:**
- Complex algorithms
- Business-critical logic
- Frequently changed code
- Bug-prone areas

**2. Before Refactoring:**
- Add tests if coverage <80%
- Focus on public API
- Test edge cases and error paths

**3. During Refactoring:**
- Coverage should not decrease
- May need new tests for extracted functions
- Delete tests for removed code

---

## Regression Prevention

### Regression Testing Strategy

**Definition:** Tests ensuring old bugs don't reappear

**Process:**
1. Bug discovered
2. Write test reproducing bug (RED)
3. Fix bug (GREEN)
4. Test now prevents regression
5. Commit test + fix together

**Example:**
```javascript
// Bug report: "Calculator divides by zero without error"

// 1. Write failing test
test('throws on division by zero', () => {
  expect(() => calc(5, 0, '/')).toThrow('Division by zero');
});

// 2. Fix the bug
function calc(a, b, op) {
  if (op === '/' && b === 0) {
    throw new Error('Division by zero');
  }
  return operations[op](a, b);
}

// 3. Test now passes and prevents regression
```

### Test Suites for Refactoring

**Unit Tests:**
- Pure functions
- Business logic
- Algorithms
- **Coverage:** 80-100%

**Integration Tests:**
- Component interactions
- Database operations
- API calls
- **Coverage:** 60-80%

**End-to-End Tests:**
- Critical user journeys
- Complete workflows
- **Coverage:** 20-30% (expensive, slow)

### Test Organization

```
tests/
├── unit/
│   ├── utils.test.js
│   ├── models.test.js
│   └── services.test.js
├── integration/
│   ├── api.test.js
│   └── database.test.js
├── e2e/
│   └── checkout.test.js
├── characterization/
│   └── legacy.test.js
└── snapshots/
    └── components.test.js.snap
```

---

## Finding Specific Content

Search patterns for quick reference:

```bash
# Find characterization test info
grep -i "characterization" references/testing-strategies.md

# Find TDD cycle
grep -i "red-green-refactor" references/testing-strategies.md

# Find snapshot testing
grep -i "snapshot" references/testing-strategies.md

# Find coverage strategies
grep -i "coverage" references/testing-strategies.md
```

---

**Sources:**
- Working Effectively with Legacy Code (Michael Feathers)
- Test-Driven Development by Example (Kent Beck)
- Industry best practices (2024-2025)
