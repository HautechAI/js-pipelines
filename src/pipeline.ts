export enum TaskStatus {
  PENDING = "pending",
  COMPLETED = "completed",
  FAILED = "failed",
}

export enum PipelineStatus {
  PENDING = "pending",
  IN_PROGRESS = "in_progress",
  COMPLETED = "completed",
  FAILED = "failed",
}

type NodeDefinition = OutputNode | TaskNode;

type OutputNode = {
  id: "output";
  result: WrapRefOrValue<any>;
};

type TaskNode = {
  id: string;
  method: string[];
  args: any[];
  dependencies: string[];
};

export type TaskOutput<T> = {
  id: string;
  result: T;
  status: () => TaskStatus;
  unwrap: () => Promise<UnwrapRef<T>>;
  cancel: () => void;
  __taskOutput__: true;
};

interface RefPrimitive<T extends string | number | boolean> {
  __kind__: T;
}
type WrapRef<T> = //
  // if T is a primitive, return a RefPrimitive
  T extends string | number | boolean
    ? RefPrimitive<T>
    : // if T is an array, return an array of Ref<T>
    T extends Array<infer U>
    ? Array<WrapRef<U>>
    : // if T is an object, return an object with Ref<T> values
    T extends Record<any, any>
    ? { [Property in keyof T]: WrapRef<T[Property]> }
    : never;

type WrapRefOrValue<T> = //
  // if T is a primitive, return a RefPrimitive
  T extends string | number | boolean
    ? RefPrimitive<T> | T
    : // if T is an array, return an array of Ref<T>
    T extends Array<infer U>
    ? Array<WrapRefOrValue<U>>
    : // if T is an object, return an object with Ref<T> values
    T extends Record<any, any>
    ? { [Property in keyof T]: WrapRefOrValue<T[Property]> }
    : never;

type UnwrapRef<T> = //
  T extends string | number | boolean
    ? T
    : T extends RefPrimitive<infer U>
    ? U
    : T extends Array<infer U>
    ? Array<UnwrapRef<U>>
    : T extends Record<any, any>
    ? { [Property in keyof T]: UnwrapRef<T[Property]> }
    : never;

type ChangeMethodSignature<T> = T extends (...args: infer P) => Promise<infer U>
  ? (
      ...args: { [K in keyof P]: WrapRefOrValue<P[K]> }
    ) => TaskOutput<WrapRef<U>>
  : never;

type ChangeMethodSignaturesInObject<T> = {
  [Property in keyof T]: T[Property] extends (...args: any[]) => any
    ? ChangeMethodSignature<T[Property]>
    : ChangeMethodSignaturesInObject<T[Property]>;
};

type PipelineState = Record<
  string,
  {
    status: TaskStatus;
    output: any;
  }
>;

export interface Methods
  extends Record<string, Methods | ((...args: any[]) => Promise<any>)> {}

const path = (obj: any, path: string[]) => {
  const res = path.reduce((acc, key) => acc?.[key], obj);
  return res;
};

export class Pipeline<T extends Methods, O = any> {
  constructor(
    private methods: T,
    private options?: {
      onChangeState?: (
        update: { taskId: string; status: TaskStatus; output: any },
        state: PipelineState
      ) => Promise<void>;
      onCompleted?: (state: PipelineState, output: O) => Promise<void>;
      serializeError?: (error: Error) => any;
      state?: PipelineState;
      tasks?: TaskNode[];
    }
  ) {
    if (options?.state) {
      this._state = options.state;
    }
    if (options?.tasks) {
      options.tasks.forEach((task) => {
        this.addNode(task);
      });
      this.newTaskIndex = this.tasks.length;
    }
  }

  public taskIdGenerator?: () => string;

  public get output(): O | null {
    const node = findOutputNode(this._tasks);
    if (node === null) {
      return null;
    }
    try {
      return replaceRefs(this._state, node);
    } catch (e) {
      // There is supposed to be a proper error handling here.
      // For some reason, when used in core, the check fails somehow.
      // Use Go next time.
      // if (e instanceof TaskNotReadyError) return null

      return null;
    }
  }

  public set output(output: WrapRefOrValue<O>) {
    if (findOutputNode(this._tasks) !== null) return;
    const outputNode: OutputNode = {
      id: "output",
      result: toPlain(output),
    };
    this.addNode(outputNode);
  }

  private _tasks: NodeDefinition[] = [];
  private _state: PipelineState = {};

  private runningTasks: Set<string> = new Set();

  private isRunning: boolean = false;

  private newTaskIndex: number = 0;
  private getNewTaskId() {
    if (this.taskIdGenerator) {
      return this.taskIdGenerator();
    }

    const ids = new Set(this._tasks.map((task) => task.id));

    let newId: string;
    do {
      newId = `task${this.newTaskIndex++}`;
    } while (ids.has(newId));
    return newId;
  }

  private addNode(task: NodeDefinition) {
    this._tasks.push(task);
  }

  private createDeferedMethods<T extends Methods>(
    methods: T,
    after: string[] = [],
    path: string[] = []
  ) {
    const self = this;

    return new Proxy<ChangeMethodSignaturesInObject<typeof methods>>(
      methods as any,
      {
        get(target, prop: string, receiver) {
          // if prop is not a function, proxy object values
          if (typeof target[prop] !== "function") {
            return self.createDeferedMethods(target[prop] as any, after, [
              ...path,
              prop.toString(),
            ]);
          }

          // if prop is a function, return a proxy function
          return (...args: any[]) => {
            const serializedArgs = toPlain(args);
            const taskId = self.getNewTaskId();
            self.addNode({
              id: taskId,
              method: [...path, prop.toString()],
              args: serializedArgs,
              dependencies: [...findRefs(serializedArgs), ...after],
            });
            return self.createTaskObject(taskId) as any;
          };
        },
      }
    );
  }

  private createTaskObject(taskId: string): TaskOutput<any> {
    const self = this;
    return {
      id: taskId,
      result: createResultReference(taskId),
      status: () => {
        return self._state[taskId]?.status ?? TaskStatus.PENDING;
      },
      unwrap: async () => {
        return await self.unwrap(createResultReference(taskId));
      },
      cancel: () => {
        self._tasks = self._tasks.filter((task) => task.id !== taskId);
        delete self._state[taskId];
      },
      __taskOutput__: true,
    };
  }

  private getTasksReadyToRun() {
    const readyTasks: TaskNode[] = [];
    for (const task of this._tasks) {
      if (
        isTaskNode(task) &&
        this._state[task.id]?.status === undefined &&
        task.dependencies.every(
          (dep) => this._state[dep]?.status === TaskStatus.COMPLETED
        ) &&
        !this.runningTasks.has(task.id)
      ) {
        readyTasks.push(task);
      }
    }
    return readyTasks;
  }

  private async runReadyTasks() {
    const tasks = this.getTasksReadyToRun();
    const promises = tasks.map(async (task) => {
      this.runningTasks.add(task.id);
      try {
        const method = path(this.methods, task.method);
        if (!method) {
          throw new MethodNotFoundError(task.method.join("."));
        }
        const result = await method(...replaceRefs(this._state, task.args));
        await this.updateState(task.id, TaskStatus.COMPLETED, result);
      } catch (e) {
        await this.updateState(
          task.id,
          TaskStatus.FAILED,
          this.options?.serializeError
            ? this.options?.serializeError(e as Error)
            : e
        );
      } finally {
        this.runningTasks.delete(task.id);
      }

      await this.runReadyTasks();
    });
    await Promise.all(promises);
  }

  private async updateState(taskId: string, status: TaskStatus, output: any) {
    this._state[taskId] = { status, output };
    await this.options?.onChangeState?.(
      { taskId, status, output },
      this._state
    );
  }

  // public methods

  after(...taskIds: string[]) {
    return this.createDeferedMethods(this.methods, taskIds);
  }

  get defer(): ChangeMethodSignaturesInObject<T> {
    return this.createDeferedMethods(this.methods);
  }

  async unwrap<T>(value: T) {
    return replaceRefs(this._state, toPlain(value)) as UnwrapRef<T>;
  }

  get status(): PipelineStatus {
    if (this.isRunning) return PipelineStatus.IN_PROGRESS;

    let hasUnprocessedTasks = false;
    for (const task of this._tasks) {
      if (!isTaskNode(task)) continue;

      if (this._state[task.id]?.status === undefined) {
        hasUnprocessedTasks = true;
      }
      if (this._state[task.id]?.status === TaskStatus.FAILED) {
        return PipelineStatus.FAILED;
      }
    }
    if (hasUnprocessedTasks) return PipelineStatus.PENDING;

    return PipelineStatus.COMPLETED;
  }

  async run() {
    this.isRunning = true;
    await this.runReadyTasks();
    this.isRunning = false;

    // resolve the output before completion
    const output = this.output as O;

    await this.options?.onCompleted?.(this._state, output);
  }

  get state() {
    return this._state as Readonly<PipelineState>;
  }

  get tasks() {
    return this._tasks as Readonly<TaskNode[]>;
  }

  loadState(state: PipelineState) {
    this._state = state;
  }

  task(taskId: string): TaskOutput<any> | null {
    if (this._tasks.find((task) => task.id === taskId))
      return this.createTaskObject(taskId);

    return null;
  }

  failedTasks() {
    return this._tasks
      .filter((task) => this._state[task.id]?.status === TaskStatus.FAILED)
      .map((task) => this.createTaskObject(task.id));
  }

  completedTasks() {
    return this._tasks
      .filter((task) => this._state[task.id]?.status === TaskStatus.COMPLETED)
      .map((task) => this.createTaskObject(task.id));
  }

  pendingTasks() {
    return this._tasks
      .filter((task) => this._state[task.id] === undefined)
      .map((task) => this.createTaskObject(task.id));
  }
}

const findOutputNode = <O>(
  nodes: NodeDefinition[]
): WrapRefOrValue<O> | null => {
  for (const node of nodes) {
    if (node.id === "output") {
      return (node as OutputNode).result;
    }
  }
  return null;
};

const isTaskNode = (node: NodeDefinition): node is TaskNode => {
  const reservedNodes: Set<string> = new Set(["output"]);
  return !reservedNodes.has(node.id);
};

// replace proxy objects with plain objects
const toPlain = (value: any) => {
  return JSON.parse(JSON.stringify(value));
};

const findRefs = (obj: any): string[] => {
  if (Array.isArray(obj)) {
    return obj.map(findRefs).flat();
  }
  if (typeof obj === "object") {
    if (obj.$ref !== undefined) {
      return [obj.$ref];
    } else {
      return Object.values(obj).map(findRefs).flat();
    }
  }
  return [];
};

const createResultReference = (taskId: string, path: string[] = []) => {
  const obj = { $ref: taskId, path };
  return new Proxy(obj, {
    get(target, prop, receiver) {
      if (prop === "toJSON") {
        return () => obj;
      }
      return createResultReference(taskId, [...path, prop.toString()]);
    },
  }) as any;
};

const replaceRefs = (state: PipelineState, obj: any): any => {
  if (Array.isArray(obj)) {
    return obj.map((x) => replaceRefs(state, x));
  }

  if (typeof obj === "object") {
    // if obj has $ref property, replace it with the actual value
    if (obj.$ref !== undefined) {
      const task = state[obj.$ref];
      if (task === undefined) {
        throw new TaskNotReadyError(obj.$ref);
      } else if (task.status === TaskStatus.FAILED) {
        throw task.output;
      } else {
        // return output value at path
        if (obj.path.length === 0) {
          return task.output;
        } else {
          return path(task.output, obj.path);
        }
      }
    } else {
      // otherwise, replace refs in all the object values
      return Object.fromEntries(
        Object.entries(obj).map(([k, v]) => [k, replaceRefs(state, v)])
      );
    }
  }
  return obj;
};

export class PipelineError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message);
    this.name = "PipelineError";
    if (options?.cause) {
      this.cause = options.cause;
    }
  }
}

export class MethodNotFoundError extends PipelineError {
  constructor(method: string, options?: ErrorOptions) {
    super(`Method ${method} not found`);
    this.name = "MethodNotFoundError";
    if (options?.cause) {
      this.cause = options.cause;
    }
  }
}

export class TaskNotReadyError extends PipelineError {
  constructor(taskId: string, options?: ErrorOptions) {
    super(`Task ${taskId} is not ready`);
    this.name = "TaskNotReadyError";
    if (options?.cause) {
      this.cause = options.cause;
    }
  }
}
