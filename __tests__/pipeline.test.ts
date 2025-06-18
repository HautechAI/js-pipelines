import { Pipeline, PipelineStatus, TaskStatus } from "../src";
import { Methods } from "./fixtures/methods";

describe("Empty pipeline", () => {
  it("should have success status without run", async () => {
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

describe("Pipeline with a single method", () => {
  const initPipeline = () => {
    const pipeline = new Pipeline(Methods);

    const task1 = pipeline.defer.generateNumber();

    return { task1, pipeline };
  };

  it("should have one task", async () => {
    const { pipeline } = initPipeline();
    expect(pipeline.tasks.length).toBe(1);
    expect(pipeline.tasks[0].method[0]).toBe("generateNumber");
  });

  it("should have empty state before running", async () => {
    const { pipeline } = initPipeline();
    expect(pipeline.state).toEqual({});
  });

  it("should have pending status before running", async () => {
    const { pipeline } = initPipeline();
    expect(pipeline.status).toBe(PipelineStatus.PENDING);
  });

  it("should successfully execute", async () => {
    const { pipeline, task1 } = initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(pipeline.unwrap(task1.result)).resolves.toBe(42);
  });

  it("should allow to get task by id", async () => {
    const { pipeline, task1 } = initPipeline();
    expect(pipeline.task(task1.id).id).toBe(task1.id);
  });

  it("should throw on attempt to get task by unknown id", async () => {
    const { pipeline, task1 } = initPipeline();
    expect(() => pipeline.task("test")).toThrow();
  });

  it("should allow to get failed tasks", async () => {
    const { pipeline, task1 } = initPipeline();

    await pipeline.run();
    expect(pipeline.failedTasks()).toEqual([]);
  });

  it("should allow to get completed tasks", async () => {
    const { pipeline, task1 } = initPipeline();

    await pipeline.run();
    expect(pipeline.completedTasks()).toEqual(
      expect.arrayContaining([expect.objectContaining({ id: task1.id })])
    );
  });
});

describe("Pipeline with two sequential methods", () => {
  const initPipeline = () => {
    const onChangeState = jest.fn();
    const pipeline = new Pipeline(Methods, {
      onChangeState: (change) => onChangeState(change),
    });

    const task1 = pipeline.defer.generateNumber();
    const task2 = pipeline.defer.multiply(task1.result, 2);

    return { task1, task2, onChangeState, pipeline };
  };

  it("should have two tasks", async () => {
    const { pipeline } = initPipeline();
    expect(pipeline.tasks.length).toBe(2);
    expect(pipeline.tasks[0].method[0]).toBe("generateNumber");
    expect(pipeline.tasks[1].method[0]).toBe("multiply");
  });

  it("should successfully execute", async () => {
    const { pipeline, task2 } = initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(pipeline.unwrap(task2.result)).resolves.toBe(84);
  });

  it("should call changeStateHandler in the order of tasks", async () => {
    const { pipeline, onChangeState } = initPipeline();

    await pipeline.run();
    expect(onChangeState).toHaveBeenNthCalledWith(1, {
      taskId: "task0",
      status: TaskStatus.COMPLETED,
      output: 42,
    });
    expect(onChangeState).toHaveBeenNthCalledWith(2, {
      taskId: "task1",
      status: TaskStatus.COMPLETED,
      output: 84,
    });
  });

  it("should successfully run with last task cancelled", async () => {
    const { pipeline, task1, task2 } = initPipeline();

    task2.cancel();
    await pipeline.run();

    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(pipeline.unwrap(task1.result)).resolves.toBe(42);
    expect(pipeline.unwrap(task2.result)).rejects.toThrow();
  });

  it("should allow to get failed tasks", async () => {
    const { pipeline } = initPipeline();

    await pipeline.run();
    expect(pipeline.failedTasks()).toEqual([]);
  });

  it("should allow to get completed tasks", async () => {
    const { pipeline, task1, task2 } = initPipeline();

    await pipeline.run();
    expect(pipeline.completedTasks()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: task1.id }),
        expect.objectContaining({ id: task2.id }),
      ])
    );
  });
});

describe("Pipeline with nested methods", () => {
  const initPipeline = () => {
    const pipeline = new Pipeline(Methods);

    const task1 = pipeline.defer.nested.generateString.v1();

    return { task1, pipeline };
  };

  it("should have one task", async () => {
    const { pipeline } = initPipeline();
    expect(pipeline.tasks.length).toBe(1);
    expect(pipeline.tasks[0].method).toEqual([
      "nested",
      "generateString",
      "v1",
    ]);
  });

  it("should successfully execute", async () => {
    const { pipeline, task1 } = initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(pipeline.unwrap(task1.result)).resolves.toBe("test-string");
    await expect(task1.unwrap()).resolves.toBe("test-string");
  });
});

describe("Pipeline with a method that throws an error", () => {
  const initPipeline = () => {
    const pipeline = new Pipeline(Methods);

    const task1 = pipeline.defer.generateNumber();
    const task2 = pipeline.defer.methodWithError(task1.result);
    const task3 = pipeline.defer.multiply(task2.result, 2);

    return { task1, task2, task3, pipeline };
  };

  it("should fail with an error", async () => {
    const { pipeline, task3 } = initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.FAILED);
    expect(pipeline.state.task1.status).toBe("failed");
    expect(pipeline.unwrap(task3.result)).rejects.toThrow();
    await expect(task3.unwrap()).rejects.toThrow();
  });

  it("should allow to get failed tasks", async () => {
    const { pipeline, task2, task3 } = initPipeline();

    await pipeline.run();
    expect(pipeline.failedTasks()).toEqual(
      expect.arrayContaining([expect.objectContaining({ id: task2.id })])
    );
  });

  it("should allow to get completed tasks", async () => {
    const { pipeline, task1 } = initPipeline();

    await pipeline.run();
    expect(pipeline.completedTasks()).toEqual(
      expect.arrayContaining([expect.objectContaining({ id: task1.id })])
    );
  });

  it("should allow to get pending tasks", async () => {
    const { pipeline, task3 } = initPipeline();

    await pipeline.run();
    expect(pipeline.pendingTasks()).toEqual(
      expect.arrayContaining([expect.objectContaining({ id: task3.id })])
    );
  });

  it("should return failed task status", async () => {
    const { pipeline, task2 } = initPipeline();

    await pipeline.run();
    expect(task2.status()).toEqual(TaskStatus.FAILED);
  });

  it("should return completed task status", async () => {
    const { pipeline, task1 } = initPipeline();

    await pipeline.run();
    expect(task1.status()).toEqual(TaskStatus.COMPLETED);
  });

  it("should return pending task status", async () => {
    const { pipeline, task3 } = initPipeline();

    await pipeline.run();
    expect(task3.status()).toEqual(TaskStatus.PENDING);
  });
});

describe("Pipeline with two instant methods", () => {
  const initPipeline = () => {
    const onChangeState = jest.fn();

    const pipeline = new Pipeline(Methods, {
      onChangeState: (change) => onChangeState(change),
    });

    const task1 = pipeline.defer.multiply(1, 2);
    const task2 = pipeline.defer.generateNumber();

    return { task1, task2, onChangeState, pipeline };
  };

  it("should call changeStateHandler in the same order as the tasks are defined", async () => {
    const { pipeline, onChangeState } = initPipeline();

    await pipeline.run();

    expect(onChangeState).toHaveBeenNthCalledWith(1, {
      taskId: "task0",
      status: TaskStatus.COMPLETED,
      output: 2,
    });
    expect(onChangeState).toHaveBeenNthCalledWith(2, {
      taskId: "task1",
      status: TaskStatus.COMPLETED,
      output: 42,
    });
  });
});

describe("Pipeline with a method that sleeps", () => {
  const initPipeline = () => {
    const onChangeState = jest.fn();

    const pipeline = new Pipeline(Methods, {
      onChangeState: (change) => onChangeState(change),
    });

    const task1 = pipeline.defer.methodWithSleep();
    const task2 = pipeline.defer.generateNumber();

    return { task1, task2, onChangeState, pipeline };
  };

  it("should call changeStateHandler in the order of task completion", async () => {
    const { pipeline, onChangeState } = initPipeline();

    await pipeline.run();

    expect(onChangeState).toHaveBeenNthCalledWith(1, {
      taskId: "task1",
      status: TaskStatus.COMPLETED,
      output: 42,
    });
    expect(onChangeState).toHaveBeenNthCalledWith(2, {
      taskId: "task0",
      status: TaskStatus.COMPLETED,
      output: "slept",
    });
  });

  it("should show running state for the pipeline", async () => {
    const { pipeline } = initPipeline();

    pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.IN_PROGRESS);
  });
});

describe("Pipeline with a method that sleeps and instant method dependant on it", () => {
  const initPipeline = () => {
    const pipeline = new Pipeline(Methods);

    const task1 = pipeline.defer.methodWithSleep();
    const task2 = pipeline.defer.concat(task1.result, " well");

    return { task1, task2, pipeline };
  };

  it("should successfully execute", async () => {
    const { pipeline, task2 } = initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(pipeline.unwrap(task2.result)).resolves.toBe("slept well");
  });
});

describe("Pipeline with restored tasks and empty state", () => {
  const initPipeline = () => {
    const initialPipeline = new Pipeline(Methods);

    const task1 = initialPipeline.defer.methodWithSleep();
    const task2 = initialPipeline.defer.concat(task1.result, " well");

    const pipeline = new Pipeline(Methods, {
      tasks: [...initialPipeline.tasks],
    });

    return { task1, task2, pipeline };
  };

  it("should successfully execute", async () => {
    const { pipeline, task2 } = initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(pipeline.unwrap(task2.result)).resolves.toBe("slept well");
  });
});

describe("Pipeline with restored tasks and state", () => {
  const initPipeline = async () => {
    const initialPipeline = new Pipeline(Methods);

    const task1 = initialPipeline.defer.methodWithSleep();
    const task2 = initialPipeline.defer.concat(task1.result, " well");

    await initialPipeline.run();

    const pipeline = new Pipeline(Methods, {
      state: initialPipeline.state,
      tasks: [...initialPipeline.tasks],
    });

    return { task1, task2, pipeline };
  };

  it("should be immediately successful", async () => {
    const { pipeline, task2 } = await initPipeline();

    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(pipeline.unwrap(task2.result)).resolves.toBe("slept well");
  });
});

describe("Pipeline with restored tasks and partially completed state", () => {
  const initPipeline = async () => {
    const initialPipeline = new Pipeline(Methods);

    const task1 = initialPipeline.defer.methodWithSleep();

    await initialPipeline.run();

    const pipeline = new Pipeline(Methods, {
      state: initialPipeline.state,
      tasks: [...initialPipeline.tasks],
    });

    const task2 = pipeline.defer.concat(task1.result, " well");

    return { task1, task2, pipeline };
  };

  it("should have pending status", async () => {
    const { pipeline, task2 } = await initPipeline();

    expect(pipeline.status).toBe(PipelineStatus.PENDING);
    expect(pipeline.unwrap(task2.result)).rejects.toThrow();
  });

  it("should successfully continue execution", async () => {
    const { pipeline, task2 } = await initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.COMPLETED);
    expect(pipeline.unwrap(task2.result)).resolves.toBe("slept well");
  });
});

describe("Pipeline with missing method", () => {
  const initPipeline = async () => {
    const initialPipeline = new Pipeline(Methods);

    const task1 = initialPipeline.defer.nested.generateString.v1();

    const pipeline = new Pipeline(
      {},
      {
        state: initialPipeline.state,
        tasks: [...initialPipeline.tasks],
      }
    );

    return { task1, pipeline };
  };

  it("should fail to run", async () => {
    const { pipeline, task1 } = await initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.FAILED);
    expect(pipeline.unwrap(task1.result)).rejects.toThrow(
      "Method nested.generateString.v1 not found"
    );
  });
});

describe("Pipeline with a method which returns an object", () => {
  const initPipeline = () => {
    const pipeline = new Pipeline(Methods);

    const task1 = pipeline.defer.generateObject();

    return { task1, pipeline };
  };

  it("should allow to access nested field", async () => {
    const { pipeline, task1 } = initPipeline();

    await pipeline.run();
    expect(pipeline.unwrap(task1.result.num)).resolves.toBe(42);
    expect((await task1.unwrap()).num).toBe(42);
  });
});

describe("Pipeline with a method which receives an object", () => {
  it("should allow to pass reference in nested fields", async () => {
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

describe("Pipeline with explicitly defined order", () => {
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

  it("should call changeStateHandler in the order of execution with respect of defined order", async () => {
    const { pipeline, onChangeState } = initPipeline();

    await pipeline.run();
    expect(onChangeState).toHaveBeenNthCalledWith(1, {
      taskId: "task0",
      status: TaskStatus.COMPLETED,
      output: 42,
    });
    expect(onChangeState).toHaveBeenNthCalledWith(2, {
      taskId: "task2",
      status: TaskStatus.COMPLETED,
      output: 42,
    });
    expect(onChangeState).toHaveBeenNthCalledWith(3, {
      taskId: "task1",
      status: TaskStatus.COMPLETED,
      output: 42,
    });
  });

  describe("Pipeline with serialize error handler", () => {
    const initPipeline = () => {
      const pipeline = new Pipeline(Methods, {
        serializeError: (e) => `Serialized error: ${e.message}`,
      });

      const task1 = pipeline.defer.generateNumber();
      const task2 = pipeline.defer.methodWithError(task1.result);

      return { task1, task2, pipeline };
    };

    it("should fail with an error", async () => {
      const { pipeline, task2 } = initPipeline();

      await pipeline.run();
      expect(pipeline.status).toBe(PipelineStatus.FAILED);
      expect(pipeline.state.task1.status).toBe("failed");
      await expect(task2.unwrap()).rejects.toBe(
        "Serialized error: Error in method"
      );
    });
  });

  describe("Load state", () => {
    const createPipeline = () => {
      const pipeline = new Pipeline(Methods);

      const task1 = pipeline.defer.generateNumber();
      const task2 = pipeline.defer.methodWithError(task1.result);

      return { task1, task2, pipeline };
    };

    it("should load state", async () => {
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


describe("Pipeline with an output", () => {
  const createPipeline = () => {
    const pipeline = new Pipeline(Methods);

    const task1 = pipeline.defer.generateNumber();
    const task2 = pipeline.defer.multiply(task1.result, 2);
    const task3 = pipeline.defer.concat(pipeline.defer.toString(task2.result).result, " is the answer");

    pipeline.output = task3.result

    return { pipeline };
  };

  it('should resolve the output successfully', async () => {
    const { pipeline } = createPipeline();
    await pipeline.run();

    expect(pipeline.output).toBe('84 is the answer');
  });
})