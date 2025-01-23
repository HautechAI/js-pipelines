# Pipeline Task Orchestration Library

## Overview

A TypeScript library for creating, managing, and orchestrating asynchronous tasks in a dependency-aware "pipeline." This library allows you to:

- Define methods (async functions) that can be queued as tasks.
- Chain tasks by referencing the output of other tasks.
- Automatically resolve dependencies and run tasks in the correct order.
- Inspect task statuses (pending, in-progress, completed, failed).
- Retrieve (unwrap) final results from tasks.

## Table of Contents

1. [Installation](#installation)
2. [Key Concepts](#key-concepts)
3. [Usage](#usage)

   - [Basic Example](#basic-example)
   - [Error Handling](#error-handling)

4. [API Reference](#api-reference)
   - [Constructor](#constructor)
   - [Methods](#methods)

## Installation

To install this package, you can use npm or yarn:

```bash
npm install @hautechai/pipelines
```

or

```bash
yarn add @hautechai/pipelines
```

## Key Concepts

### Tasks

- Each task is associated with one of your defined methods.
- A task can depend on other tasks. The library automatically ensures that dependent tasks only start once all of their dependencies have completed.
- Each task has a status: pending, completed, or failed.

### References

- If Task A depends on the result of Task B, you can reference Task B’s result in Task A’s arguments.
- Internally, this reference is a kind of "placeholder" that will be resolved when the pipeline runs.

### Pipeline

- A Pipeline manages tasks, maintains state, and orchestrates the execution order.
- You queue tasks using either pipeline.defer or pipeline.after(...taskIds).
- The pipeline runs tasks in topological order (i.e., respecting dependencies).

## Usage

### Basic Example

```javascript
import { Pipeline, Methods } from "@hautechai/pipelines";

// Define your methods
const Methods = {
  async generateNumber() {
    return 42;
  },
  async multiply(number, factor) {
    return number * factor;
  },
};

// Create a new pipeline
const pipeline = new Pipeline(Methods);

// Add tasks to the pipeline
const task1 = pipeline.defer.generateNumber();
const task2 = pipeline.defer.multiply(task1.result, 2);

// Run the pipeline
(async () => {
  await pipeline.run();
  console.log(`Result: ${await pipeline.unwrap(task2.result)}`); // Result should be 84
})();
```

### Error Handling

The pipeline can gracefully handle errors:

```javascript
import { Pipeline, Methods } from "@hautechai/pipelines";

// Define your methods
const Methods = {
  async generateNumber() {
    return 42;
  },
  async methodWithError() {
    throw new Error("Error in method");
  },
};

// Create a new pipeline
const pipeline = new Pipeline(Methods);

const task1 = pipeline.defer.generateNumber();
const task2 = pipeline.defer.methodWithError(task1.result);

(async () => {
  await pipeline.run();
  if (pipeline.status === PipelineStatus.FAILED) {
    console.error("Pipeline failed:", pipeline.state);
  }
})();
```

## API Reference

#### Constructor

- `new Pipeline(methods: Methods, options?: { onChangeState?: Function; serializeError?: Function; state?: PipelineState; tasks?: Task[]; })`
  - `methods`: An object defining methods the pipeline can execute.
  - `options`: Optional settings for the pipeline.

#### Methods

- `after(...taskIds: string[])`: Create deferred methods that depend on the completion of specified tasks.
- `run()`: Start executing the pipeline.
- `status`: Get the current pipeline status.
- `state`: Get the current state of the pipeline.
- `tasks`: Get the list of current tasks.
- `loadState(state: PipelineState)`: Load a previously saved state into the pipeline.
- `unwrap(value: T)`: Unwrap reference values to their actual results.
