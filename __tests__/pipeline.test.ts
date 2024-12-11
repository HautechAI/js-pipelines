import { Pipeline, PipelineStatus } from "../src";
import { Methods } from "./fixtures/methods";

describe("Empty pipeline", () => {
  it("should have success status without run", async () => {
    const pipeline = new Pipeline(Methods);
    expect(pipeline.status).toBe(PipelineStatus.Success);
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
    expect(pipeline.status).toBe(PipelineStatus.Pending);
  });

  it("should successfully execute", async () => {
    const { pipeline, task1 } = initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.Success);
    expect(pipeline.wait(task1.result)).resolves.toBe(42);
  });

  it("should allow to get task by id", async () => {
    const { pipeline, task1 } = initPipeline();
    expect(pipeline.task(task1.id).id).toBe(task1.id);
  });

  it("should throw on attempt to get task by unknown id", async () => {
    const { pipeline, task1 } = initPipeline();
    expect(() => pipeline.task("test")).toThrow();
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
    expect(pipeline.status).toBe(PipelineStatus.Success);
    expect(pipeline.wait(task2.result)).resolves.toBe(84);
  });

  it("should call changeStateHandler in the order of tasks", async () => {
    const { pipeline, onChangeState } = initPipeline();

    await pipeline.run();
    expect(onChangeState).toHaveBeenNthCalledWith(1, {
      taskId: "task0",
      status: "success",
      output: 42,
    });
    expect(onChangeState).toHaveBeenNthCalledWith(2, {
      taskId: "task1",
      status: "success",
      output: 84,
    });
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
    expect(pipeline.status).toBe(PipelineStatus.Success);
    expect(pipeline.wait(task1.result)).resolves.toBe("test-string");
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
    expect(pipeline.status).toBe(PipelineStatus.Failed);
    expect(pipeline.state.task1.status).toBe("failed");
    expect(pipeline.wait(task3.result)).rejects.toThrow();
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
      status: "success",
      output: 2,
    });
    expect(onChangeState).toHaveBeenNthCalledWith(2, {
      taskId: "task1",
      status: "success",
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
      status: "success",
      output: 42,
    });
    expect(onChangeState).toHaveBeenNthCalledWith(2, {
      taskId: "task0",
      status: "success",
      output: "slept",
    });
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
    expect(pipeline.status).toBe(PipelineStatus.Success);
    expect(pipeline.wait(task2.result)).resolves.toBe("slept well");
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
    expect(pipeline.status).toBe(PipelineStatus.Success);
    expect(pipeline.wait(task2.result)).resolves.toBe("slept well");
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

    expect(pipeline.status).toBe(PipelineStatus.Success);
    expect(pipeline.wait(task2.result)).resolves.toBe("slept well");
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

    expect(pipeline.status).toBe(PipelineStatus.Pending);
    expect(pipeline.wait(task2.result)).rejects.toThrow();
  });

  it("should successfully continue execution", async () => {
    const { pipeline, task2 } = await initPipeline();

    await pipeline.run();
    expect(pipeline.status).toBe(PipelineStatus.Success);
    expect(pipeline.wait(task2.result)).resolves.toBe("slept well");
  });
});
