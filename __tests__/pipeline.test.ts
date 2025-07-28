import {
  Pipeline,
  PipelineStatus,
  TaskNotReadyError,
  TaskStatus,
} from '../src';
import { Methods } from './fixtures/methods';

describe('Empty pipeline', () => {
  it('should have success status without run', async () => {
    const pipeline = new Pipeline(Methods);
    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
  });

  it('should call onCompleted', async () => {
    const onCompleted = jest.fn();
    const pipeline = new Pipeline(Methods, {
      onCompleted: (state) => onCompleted(state),
    });
    await pipeline.run();
    expect(onCompleted).toHaveBeenCalled();
  });
});

describe('Pipeline with a single method', () => {
  const initPipeline = () => {
    const pipeline = new Pipeline(Methods);

    const task1 = pipeline.defer.generateNumber();

    return { task1, pipeline };
  };

  it('should have one task', async () => {
    const { pipeline } = initPipeline();
    expect(pipeline.tasks.length).toBe(1);
    expect(pipeline.tasks[0].method[0]).toBe('generateNumber');
  });

  it('should have empty state before running', async () => {
    const { pipeline } = initPipeline();
    expect(pipeline.state).toEqual({});
  });

  it('should have pending status before running', async () => {
    const { pipeline } = initPipeline();
    expect(pipeline.status).toBe(PipelineStatus.PENDING);
  });

  it('should successfully execute', async () => {
    const { pipeline, task1 } = initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(pipeline.unwrap(task1.result)).resolves.toBe(42);
  });

  it('should allow to get task by id', async () => {
    const { pipeline, task1 } = initPipeline();
    expect(pipeline.task(task1.id)!.id).toBe(task1.id);
  });

  it.skip('should throw on attempt to get task by unknown id', async () => {
    const { pipeline, task1 } = initPipeline();
    expect(() => pipeline.task('test')).toThrow();
  });

  it('should return null on attempt to get task by unknown id', async () => {
    const { pipeline, task1 } = initPipeline();
    expect(pipeline.task('test')).toEqual(null);
  });

  it('should allow to get failed tasks', async () => {
    const { pipeline, task1 } = initPipeline();

    await pipeline.run();
    expect(pipeline.failedTasks()).toEqual([]);
  });

  it('should allow to get completed tasks', async () => {
    const { pipeline, task1 } = initPipeline();

    await pipeline.run();
    expect(pipeline.completedTasks()).toEqual(
      expect.arrayContaining([expect.objectContaining({ id: task1.id })]),
    );
  });
});

describe('Pipeline with two sequential methods', () => {
  const initPipeline = () => {
    const onChangeState = jest.fn();
    const pipeline = new Pipeline(Methods, {
      onChangeState: (change) => onChangeState(change),
    });

    const task1 = pipeline.defer.generateNumber();
    const task2 = pipeline.defer.multiply(task1.result, 2);

    return { task1, task2, onChangeState, pipeline };
  };

  it('should have two tasks', async () => {
    const { pipeline } = initPipeline();
    expect(pipeline.tasks.length).toBe(2);
    expect(pipeline.tasks[0].method[0]).toBe('generateNumber');
    expect(pipeline.tasks[1].method[0]).toBe('multiply');
  });

  it('should successfully execute', async () => {
    const { pipeline, task2 } = initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(pipeline.unwrap(task2.result)).resolves.toBe(84);
  });

  it('should call changeStateHandler in the order of tasks', async () => {
    const { pipeline, onChangeState } = initPipeline();

    await pipeline.run();
    expect(onChangeState).toHaveBeenNthCalledWith(1, {
      taskId: 'task0',
      status: TaskStatus.COMPLETED,
      output: 42,
    });
    expect(onChangeState).toHaveBeenNthCalledWith(2, {
      taskId: 'task1',
      status: TaskStatus.COMPLETED,
      output: 84,
    });
  });

  it('should successfully run with last task cancelled', async () => {
    const { pipeline, task1, task2 } = initPipeline();

    task2.cancel();
    await pipeline.run();

    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(pipeline.unwrap(task1.result)).resolves.toBe(42);
    expect(pipeline.unwrap(task2.result)).rejects.toThrow();
  });

  it('should allow to get failed tasks', async () => {
    const { pipeline } = initPipeline();

    await pipeline.run();
    expect(pipeline.failedTasks()).toEqual([]);
  });

  it('should allow to get completed tasks', async () => {
    const { pipeline, task1, task2 } = initPipeline();

    await pipeline.run();
    expect(pipeline.completedTasks()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: task1.id }),
        expect.objectContaining({ id: task2.id }),
      ]),
    );
  });
});

describe('Pipeline with nested methods', () => {
  const initPipeline = () => {
    const pipeline = new Pipeline(Methods);

    const task1 = pipeline.defer.nested.generateString.v1();

    return { task1, pipeline };
  };

  it('should have one task', async () => {
    const { pipeline } = initPipeline();
    expect(pipeline.tasks.length).toBe(1);
    expect(pipeline.tasks[0].method).toEqual([
      'nested',
      'generateString',
      'v1',
    ]);
  });

  it('should successfully execute', async () => {
    const { pipeline, task1 } = initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(pipeline.unwrap(task1.result)).resolves.toBe('test-string');
    await expect(task1.unwrap()).resolves.toBe('test-string');
  });
});

describe('Pipeline with a method that throws an error', () => {
  const initPipeline = () => {
    const pipeline = new Pipeline(Methods);

    const task1 = pipeline.defer.generateNumber();
    const task2 = pipeline.defer.methodWithError(task1.result);
    const task3 = pipeline.defer.multiply(task2.result, 2);

    return { task1, task2, task3, pipeline };
  };

  it('should fail with an error', async () => {
    const { pipeline, task3 } = initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.FAILED);
    expect(pipeline.state.task1.status).toBe('failed');
    expect(pipeline.unwrap(task3.result)).rejects.toThrow();
    await expect(task3.unwrap()).rejects.toThrow();
  });

  it('should allow to get failed tasks', async () => {
    const { pipeline, task2, task3 } = initPipeline();

    await pipeline.run();
    expect(pipeline.failedTasks()).toEqual(
      expect.arrayContaining([expect.objectContaining({ id: task2.id })]),
    );
  });

  it('should allow to get completed tasks', async () => {
    const { pipeline, task1 } = initPipeline();

    await pipeline.run();
    expect(pipeline.completedTasks()).toEqual(
      expect.arrayContaining([expect.objectContaining({ id: task1.id })]),
    );
  });

  it('should allow to get pending tasks', async () => {
    const { pipeline, task3 } = initPipeline();

    await pipeline.run();
    expect(pipeline.pendingTasks()).toEqual(
      expect.arrayContaining([expect.objectContaining({ id: task3.id })]),
    );
  });

  it('should return failed task status', async () => {
    const { pipeline, task2 } = initPipeline();

    await pipeline.run();
    expect(task2.status()).toEqual(TaskStatus.FAILED);
  });

  it('should return completed task status', async () => {
    const { pipeline, task1 } = initPipeline();

    await pipeline.run();
    expect(task1.status()).toEqual(TaskStatus.COMPLETED);
  });

  it('should return pending task status', async () => {
    const { pipeline, task3 } = initPipeline();

    await pipeline.run();
    expect(task3.status()).toEqual(TaskStatus.PENDING);
  });
});

describe('Pipeline with two instant methods', () => {
  const initPipeline = () => {
    const onChangeState = jest.fn();

    const pipeline = new Pipeline(Methods, {
      onChangeState: (change) => onChangeState(change),
    });

    const task1 = pipeline.defer.multiply(1, 2);
    const task2 = pipeline.defer.generateNumber();

    return { task1, task2, onChangeState, pipeline };
  };

  it('should call changeStateHandler in the same order as the tasks are defined', async () => {
    const { pipeline, onChangeState } = initPipeline();

    await pipeline.run();

    expect(onChangeState).toHaveBeenNthCalledWith(1, {
      taskId: 'task0',
      status: TaskStatus.COMPLETED,
      output: 2,
    });
    expect(onChangeState).toHaveBeenNthCalledWith(2, {
      taskId: 'task1',
      status: TaskStatus.COMPLETED,
      output: 42,
    });
  });
});

describe('Pipeline with a method that sleeps', () => {
  const initPipeline = () => {
    const onChangeState = jest.fn();

    const pipeline = new Pipeline(Methods, {
      onChangeState: (change) => onChangeState(change),
    });

    const task1 = pipeline.defer.methodWithSleep();
    const task2 = pipeline.defer.generateNumber();

    return { task1, task2, onChangeState, pipeline };
  };

  it('should call changeStateHandler in the order of task completion', async () => {
    const { pipeline, onChangeState } = initPipeline();

    await pipeline.run();

    expect(onChangeState).toHaveBeenNthCalledWith(1, {
      taskId: 'task1',
      status: TaskStatus.COMPLETED,
      output: 42,
    });
    expect(onChangeState).toHaveBeenNthCalledWith(2, {
      taskId: 'task0',
      status: TaskStatus.COMPLETED,
      output: 'slept',
    });
  });

  it('should show running state for the pipeline', async () => {
    const { pipeline } = initPipeline();

    pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.IN_PROGRESS);
  });
});

describe('Pipeline with a method that sleeps and instant method dependant on it', () => {
  const initPipeline = () => {
    const pipeline = new Pipeline(Methods);

    const task1 = pipeline.defer.methodWithSleep();
    const task2 = pipeline.defer.concat(task1.result, ' well');

    return { task1, task2, pipeline };
  };

  it('should successfully execute', async () => {
    const { pipeline, task2 } = initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(pipeline.unwrap(task2.result)).resolves.toBe('slept well');
  });
});

describe('Pipeline with restored tasks and empty state', () => {
  const initPipeline = () => {
    const initialPipeline = new Pipeline(Methods);

    const task1 = initialPipeline.defer.methodWithSleep();
    const task2 = initialPipeline.defer.concat(task1.result, ' well');

    const pipeline = new Pipeline(Methods, {
      tasks: [...initialPipeline.tasks],
    });

    return { task1, task2, pipeline };
  };

  it('should successfully execute', async () => {
    const { pipeline, task2 } = initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(pipeline.unwrap(task2.result)).resolves.toBe('slept well');
  });
});

describe('Pipeline with restored tasks and state', () => {
  const initPipeline = async () => {
    const initialPipeline = new Pipeline(Methods);

    const task1 = initialPipeline.defer.methodWithSleep();
    const task2 = initialPipeline.defer.concat(task1.result, ' well');

    await initialPipeline.run();

    const pipeline = new Pipeline(Methods, {
      state: initialPipeline.state,
      tasks: [...initialPipeline.tasks],
    });

    return { task1, task2, pipeline };
  };

  it('should be immediately successful', async () => {
    const { pipeline, task2 } = await initPipeline();

    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(pipeline.unwrap(task2.result)).resolves.toBe('slept well');
  });
});

describe('Pipeline with restored tasks and partially completed state', () => {
  const initPipeline = async () => {
    const initialPipeline = new Pipeline(Methods);

    const task1 = initialPipeline.defer.methodWithSleep();

    await initialPipeline.run();

    const pipeline = new Pipeline(Methods, {
      state: initialPipeline.state,
      tasks: [...initialPipeline.tasks],
    });

    const task2 = pipeline.defer.concat(task1.result, ' well');

    return { task1, task2, pipeline };
  };

  it('should have pending status', async () => {
    const { pipeline, task2 } = await initPipeline();

    expect(pipeline.status).toBe(PipelineStatus.PENDING);
    expect(pipeline.unwrap(task2.result)).rejects.toThrow();
  });

  it('should successfully continue execution', async () => {
    const { pipeline, task2 } = await initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(pipeline.unwrap(task2.result)).resolves.toBe('slept well');
  });
});

describe('Pipeline with missing method', () => {
  const initPipeline = async () => {
    const initialPipeline = new Pipeline(Methods);

    const task1 = initialPipeline.defer.nested.generateString.v1();

    const pipeline = new Pipeline(
      {},
      {
        state: initialPipeline.state,
        tasks: [...initialPipeline.tasks],
      },
    );

    return { task1, pipeline };
  };

  it('should fail to run', async () => {
    const { pipeline, task1 } = await initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.FAILED);
    expect(pipeline.unwrap(task1.result)).rejects.toThrow(
      'Method nested.generateString.v1 not found',
    );
  });
});

describe('Pipeline with a method which returns an object', () => {
  const initPipeline = () => {
    const pipeline = new Pipeline(Methods);

    const task1 = pipeline.defer.generateObject();

    return { task1, pipeline };
  };

  it('should allow to access nested field', async () => {
    const { pipeline, task1 } = initPipeline();

    await pipeline.run();
    expect(pipeline.unwrap(task1.result.num)).resolves.toBe(42);
    expect((await task1.unwrap()).num).toBe(42);
  });
});

describe('Pipeline with a method which receives an object', () => {
  it('should allow to pass reference in nested fields', async () => {
    const pipeline = new Pipeline(Methods);

    const task1 = pipeline.defer.generateNumber();
    const task2 = pipeline.defer.generateNumber();
    const task3 = pipeline.defer.sumObjectFields({
      a: task1.result,
      b: task2.result,
    });

    await pipeline.run();

    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(pipeline.unwrap(task3.result)).resolves.toBe(84);
  });
});

describe('Pipeline with explicitly defined order', () => {
  const initPipeline = () => {
    const onChangeState = jest.fn();

    const pipeline = new Pipeline(Methods, {
      onChangeState: (change) => onChangeState(change),
    });

    const task1 = pipeline.defer.generateNumber();
    const task2 = pipeline.after(task1.id).generateNumber();
    const task3 = pipeline.defer.generateNumber();

    return { task1, task2, task3, pipeline, onChangeState };
  };

  it('should call changeStateHandler in the order of execution with respect of defined order', async () => {
    const { pipeline, onChangeState } = initPipeline();

    await pipeline.run();
    expect(onChangeState).toHaveBeenNthCalledWith(1, {
      taskId: 'task0',
      status: TaskStatus.COMPLETED,
      output: 42,
    });
    expect(onChangeState).toHaveBeenNthCalledWith(2, {
      taskId: 'task2',
      status: TaskStatus.COMPLETED,
      output: 42,
    });
    expect(onChangeState).toHaveBeenNthCalledWith(3, {
      taskId: 'task1',
      status: TaskStatus.COMPLETED,
      output: 42,
    });
  });

  describe('Pipeline with serialize error handler', () => {
    const initPipeline = () => {
      const pipeline = new Pipeline(Methods, {
        serializeError: (e) => `Serialized error: ${e.message}`,
      });

      const task1 = pipeline.defer.generateNumber();
      const task2 = pipeline.defer.methodWithError(task1.result);

      return { task1, task2, pipeline };
    };

    it('should fail with an error', async () => {
      const { pipeline, task2 } = initPipeline();

      await pipeline.run();
      expect(pipeline.status).toBe(PipelineStatus.FAILED);
      expect(pipeline.state.task1.status).toBe('failed');
      await expect(task2.unwrap()).rejects.toBe(
        'Serialized error: Error in method',
      );
    });
  });

  describe('Load state', () => {
    const createPipeline = () => {
      const pipeline = new Pipeline(Methods);

      const task1 = pipeline.defer.generateNumber();
      const task2 = pipeline.defer.methodWithError(task1.result);

      return { task1, task2, pipeline };
    };

    it('should load state', async () => {
      const { pipeline } = createPipeline();
      await pipeline.run();
      const state = pipeline.state;

      const { pipeline: newPipeline, task1 } = createPipeline();
      newPipeline.loadState(state);

      expect(newPipeline.status).toBe(PipelineStatus.FAILED);
      expect(task1.unwrap()).resolves.toBe(42);
    });
  });
});

describe('Pipeline with an output', () => {
  const createPipeline = () => {
    const pipeline = new Pipeline(Methods);

    const task1 = pipeline.defer.generateNumber();
    const task2 = pipeline.defer.multiply(task1.result, 2);
    const task3 = pipeline.defer.concat(
      pipeline.defer.toString(task2.result).result,
      ' is the answer',
    );

    return { task1, task2, task3, pipeline };
  };

  it('should resolve the output successfully - output is a string', async () => {
    const { pipeline, task3 } = createPipeline();
    pipeline.output = task3.result;
    await pipeline.run();

    expect(pipeline.output).toEqual('84 is the answer');
  });

  it('should resolve the output successfully - output is an object', async () => {
    const { pipeline, task3, task1 } = createPipeline();
    pipeline.output = {
      message: task3.result,
      generated: task1.result,
      note: 'This is an object',
    };
    await pipeline.run();

    expect(pipeline.output).toEqual({
      message: '84 is the answer',
      generated: 42,
      note: 'This is an object',
    });
  });

  describe('Load state', () => {
    const createPipeline = () => {
      const pipeline = new Pipeline(Methods);

      const task1 = pipeline.defer.generateNumber();
      pipeline.output = task1.result;
      return { task1, pipeline };
    };

    it('should load state', async () => {
      const { pipeline } = createPipeline();
      await pipeline.run();
      const state = pipeline.state;

      expect(pipeline.output).toEqual(42);

      const { pipeline: newPipeline } = createPipeline();
      newPipeline.loadState(state);

      expect(newPipeline.status).toBe(PipelineStatus.COMPLETED);
      expect(newPipeline.output).toEqual(42);
    });
  });
});

describe('Pipeline with taskIdGenerator', () => {
  it('should generate unique task IDs', () => {
    const pipeline = new Pipeline(Methods);
    let id = 0;
    pipeline.taskIdGenerator = () => `customTask${id++}`;
    const task1 = pipeline.defer.generateNumber();
    const task2 = pipeline.defer.generateNumber();

    expect(task1.id).toBe('customTask0');
    expect(task2.id).toBe('customTask1');
  });

  it('should allow to replace generator for some tasks', () => {
    const pipeline = new Pipeline(Methods);
    let id = 0;
    pipeline.taskIdGenerator = () => `customTask${id++}`;

    const task1 = pipeline.defer.generateNumber();
    pipeline.taskIdGenerator = () => `anotherTask${id++}`;
    const task2 = pipeline.defer.generateNumber();

    expect(task1.id).toBe('customTask0');
    expect(task2.id).toBe('anotherTask1');
  });
});

describe('Pipeline with user input', () => {
  it('should accept user input and make it accessible', async () => {
    const userInput = { name: 'John', age: 30 };
    const pipeline = new Pipeline(Methods, { input: userInput });

    expect(pipeline.input).not.toBeNull();
  });

  it('should return null when no user input is provided', async () => {
    const pipeline = new Pipeline(Methods);
    expect(pipeline.input).toBeNull();
  });

  it('should allow tasks to reference user input', async () => {
    const userInput = { a: 10, b: 20 };
    const pipeline = new Pipeline(Methods, { input: userInput });

    const task1 = pipeline.defer.sumObjectFields(pipeline.input!);
    await pipeline.run();

    expect(await pipeline.unwrap(task1.result)).toBe(30);
  });

  it('should allow tasks to reference nested user input properties', async () => {
    const userInput = { user: { name: 'Alice' }, greeting: 'Hello' };
    const pipeline = new Pipeline(Methods, { input: userInput });

    const task1 = pipeline.defer.concat(
      pipeline.input!.greeting,
      pipeline.input!.user.name,
    );
    await pipeline.run();

    expect(await pipeline.unwrap(task1.result)).toBe('HelloAlice');
  });

  it('should allow mixing user input with task results', async () => {
    const userInput = { multiplier: 3 };
    const pipeline = new Pipeline(Methods, { input: userInput });

    const task1 = pipeline.defer.generateNumber(); // returns 42
    const task2 = pipeline.defer.multiply(
      task1.result,
      pipeline.input!.multiplier,
    );
    await pipeline.run();

    expect(await pipeline.unwrap(task2.result)).toBe(126); // 42 * 3
  });

  it('should create $ref objects when accessing pipeline.inputRef', async () => {
    const userInput = { name: 'John', age: 30 };
    const pipeline = new Pipeline(Methods, { input: userInput });

    const inputRef = pipeline.inputRef!;
    const serialized = JSON.parse(JSON.stringify(inputRef));

    expect(serialized).toEqual({
      $ref: '$input',
      path: [],
    });
  });

  it('should create $ref objects with correct paths for nested input properties', async () => {
    const userInput = { user: { name: 'Alice', details: { age: 25 } } };
    const pipeline = new Pipeline(Methods, { input: userInput });

    const nameRef = pipeline.inputRef!.user.name;
    const ageRef = pipeline.inputRef!.user.details.age;

    const serializedName = JSON.parse(JSON.stringify(nameRef));
    const serializedAge = JSON.parse(JSON.stringify(ageRef));

    expect(serializedName).toEqual({
      $ref: '$input',
      path: ['user', 'name'],
    });

    expect(serializedAge).toEqual({
      $ref: '$input',
      path: ['user', 'details', 'age'],
    });
  });

  it('should resolve $input references correctly during task execution', async () => {
    const userInput = { data: { value: 100 }, multiplier: 2 };
    const pipeline = new Pipeline(Methods, { input: userInput });

    // Create tasks that use $input references
    const task1 = pipeline.defer.multiply(
      pipeline.input!.data.value,
      pipeline.input!.multiplier,
    );
    await pipeline.run();

    expect(await pipeline.unwrap(task1.result)).toBe(200); // 100 * 2
  });

  it('should init pipeline with two tasks where second task takes input from task 1 and pipeline input', async () => {
    const userInput = { baseValue: 5, multiplier: 3 };
    const pipeline = new Pipeline(Methods, { input: userInput });

    // Task 1: Generate a number (returns 42)
    const task1 = pipeline.defer.generateNumber();

    // Task 2: Takes result from task1 and multiplies by pipeline input
    const task2 = pipeline.defer.multiply(
      task1.result,
      pipeline.inputRef!.multiplier,
    );

    // Verify pipeline structure
    expect(pipeline.tasks.length).toBe(2);
    expect(pipeline.tasks[0].method[0]).toBe('generateNumber');
    expect(pipeline.tasks[1].method[0]).toBe('multiply');

    // Verify task dependencies
    expect(pipeline.tasks[0].dependencies).toEqual([]);
    expect(pipeline.tasks[1].dependencies).toEqual([task1.id, '$input']);

    // Run pipeline and verify results
    await pipeline.run();

    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(await pipeline.unwrap(task1.result)).toBe(42);
    expect(await pipeline.unwrap(task2.result)).toBe(126); // 42 * 3
  });

  it('should handle complex pipeline with two tasks using different input properties', async () => {
    const userInput = {
      config: { factor: 2 },
      settings: { offset: 10 },
    };
    const pipeline = new Pipeline(Methods, { input: userInput });

    // Task 1: Generate number and multiply by config factor
    const task1 = pipeline.defer.multiply(pipeline.input!.config.factor, 21); // 2 * 21 = 42

    // Task 2: Take task1 result and add settings offset
    const task2 = pipeline.defer.multiply(task1.result, 1); // Just pass through for now
    const task3 = pipeline.defer.sumObjectFields({
      a: task2.result,
      b: pipeline.input!.settings.offset,
    });

    await pipeline.run();

    expect(await pipeline.unwrap(task1.result)).toBe(42); // 2 * 21
    expect(await pipeline.unwrap(task2.result)).toBe(42); // pass through
    expect(await pipeline.unwrap(task3.result)).toBe(52); // 42 + 10
  });
});
