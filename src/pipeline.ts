import { MethodNotFoundError, TaskNotReadyError } from './errors';

export enum TaskStatus {
  PENDING = 'pending',
  COMPLETED = 'completed',
  FAILED = 'failed',
}

export enum PipelineStatus {
  PENDING = 'pending',
  IN_PROGRESS = 'in_progress',
  COMPLETED = 'completed',
  FAILED = 'failed',
}

type NodeDefinition = TaskNode;

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

export interface RefPrimitive<T extends string | number | boolean> {
  __kind__: T;
}
export type WrapRef<T> = //
  // if T is a primitive, return a RefPrimitive
  T extends string | number | boolean
    ? RefPrimitive<T>
    : // if T is an array, return an array of Ref<T>
      T extends Array<infer U>
      ? Array<WrapRef<U>>
      : // if T is an object, return an object with Ref<T> values
        T extends Record<any, any>
        ? { [Property in keyof T]: WrapRef<T[Property]> }
        : T;

export type WrapRefOrValue<T> = //
  // if T is a primitive, return a RefPrimitive
  T extends string | number | boolean
    ? RefPrimitive<T> | T
    : // if T is an array, return an array of Ref<T>
      T extends Array<infer U>
      ? Array<WrapRefOrValue<U>>
      : // if T is an object, return an object with Ref<T> values
        T extends Record<any, any>
        ? { [Property in keyof T]: WrapRefOrValue<T[Property]> }
        : T;

export type UnwrapRef<T> = //
  T extends string | number | boolean
    ? T
    : T extends RefPrimitive<infer U>
      ? U
      : T extends Array<infer U>
        ? Array<UnwrapRef<U>>
        : T extends Record<any, any>
          ? { [Property in keyof T]: UnwrapRef<T[Property]> }
          : never;

export type ChangeMethodSignature<T> = T extends (
  ...args: infer P
) => Promise<infer U>
  ? <J = U>(
      ...args: { [K in keyof P]: WrapRefOrValue<P[K]> }
    ) => TaskOutput<WrapRef<J>>
  : never;

export type ChangeMethodSignaturesInObject<T> = {
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

export class Pipeline<T extends Methods, O = any, I = any> {
  constructor(
    private methods: T,
    private options?: {
      onChangeState?: (
        update: { taskId: string; status: TaskStatus; output: any },
        state: PipelineState,
      ) => Promise<void>;
      onCompleted?: (state: PipelineState, output: O) => Promise<void>;
      serializeError?: (error: Error) => any;
      state?: PipelineState;
      tasks?: TaskNode[];
      input?: I;
      outputRef?: WrapRefOrValue<O>;
    },
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
    if (options?.input !== undefined) {
      this._input = options.input;
    }

    if (options?.outputRef !== undefined) {
      this._outputRef = options.outputRef;
    }
  }

  public toObject() {
    return {
      tasks: this.tasks,
      outputRef: this.getOutputRef(),
      state: this.state,
      inputRef: this.$input,
    };
  }

  public toJson() {
    return JSON.stringify(this.toObject());
  }

  public taskIdGenerator?: () => string;

  public getOutput(): O | null {
    if (!this._outputRef) {
      return null;
    }

    try {
      return replaceRefs(this._outputRef, {
        state: this._state,
        input: this._input,
      });
    } catch (e) {
      return null;
    }
  }

  public getOutputRef(): WrapRefOrValue<O> | null {
    return this._outputRef || null;
  }

  public setOutputRef(output: WrapRefOrValue<O>) {
    this._outputRef = toPlain(output);
  }

  public getInputRef(): WrapRefOrValue<I> | null {
    return this._input
      ? (createResultReference('$input') as WrapRefOrValue<I>)
      : null;
  }

  public get $input(): WrapRefOrValue<I> | null {
    return this.getInputRef();
  }

  public setInput(input: I) {
    this._input = toPlain(input);
  }

  public getInput(): I | null {
    return this._input || null;
  }

  private _tasks: NodeDefinition[] = [];
  private _state: PipelineState = {};
  private _outputRef?: WrapRefOrValue<O>;
  private _input?: I;

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
    path: string[] = [],
  ) {
    const self = this;

    return new Proxy<ChangeMethodSignaturesInObject<typeof methods>>(
      methods as any,
      {
        get(target, prop: string, receiver) {
          // if prop is not a function, proxy object values
          if (typeof target[prop] !== 'function') {
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
      },
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
        this._state[task.id]?.status === undefined &&
        task.dependencies.every(
          (dep) =>
            dep === '$input' ||
            this._state[dep]?.status === TaskStatus.COMPLETED,
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
          throw new MethodNotFoundError(task.method.join('.'));
        }
        const result = await method(
          ...replaceRefs(task.args, { state: this._state, input: this._input }),
        );
        await this.updateState(task.id, TaskStatus.COMPLETED, result);
      } catch (e) {
        await this.updateState(
          task.id,
          TaskStatus.FAILED,
          this.options?.serializeError
            ? this.options?.serializeError(e as Error)
            : e,
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
      this._state,
    );
  }

  // public methods

  after(...taskIds: string[]) {
    return this.createDeferedMethods(this.methods, taskIds);
  }

  get defer(): ChangeMethodSignaturesInObject<T> {
    return this.createDeferedMethods(this.methods);
  }

  /**
   * Unwraps the value with references into a plain value.
   * @param value The value to unwrap.
   * @returns The unwrapped value.
   */
  async unwrap<T>(value: T) {
    return replaceRefs(toPlain(value), {
      state: this._state,
      input: this._input,
    }) as UnwrapRef<T>;
  }

  get status(): PipelineStatus {
    if (this.isRunning) return PipelineStatus.IN_PROGRESS;

    let hasUnprocessedTasks = false;
    for (const task of this._tasks) {
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
    const output = this.getOutput() as O;

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

  /**
   * Returns a task by its ID.
   * @param taskId The ID of the task to retrieve.
   * @returns The task object or null if not found.
   */
  task(taskId: string): TaskOutput<any> | null {
    if (this._tasks.find((task) => task.id === taskId))
      return this.createTaskObject(taskId);

    return null;
  }

  /**
   * Returns tasks that are completed.
   * @returns The completed tasks.
   */
  failedTasks() {
    return this._tasks
      .filter((task) => this._state[task.id]?.status === TaskStatus.FAILED)
      .map((task) => this.createTaskObject(task.id));
  }

  /**
   * Returns tasks that are completed.
   * @returns The completed tasks.
   */
  completedTasks() {
    return this._tasks
      .filter((task) => this._state[task.id]?.status === TaskStatus.COMPLETED)
      .map((task) => this.createTaskObject(task.id));
  }

  /**
   * Returns tasks that are pending.
   * @returns The pending tasks.
   */
  pendingTasks() {
    return this._tasks
      .filter((task) => this._state[task.id] === undefined)
      .map((task) => this.createTaskObject(task.id));
  }

  /**
   * Compacts the tasks by removing completed tasks and replacing references with actual values.
   * @returns The compacted tasks.
   */
  compactTasks() {
    const tasksToExecute = this._tasks.filter(
      (task) => this._state[task.id]?.status !== TaskStatus.COMPLETED,
    );
    const tasksToExecuteIds = new Set(tasksToExecute.map((task) => task.id));

    const compactedTasks = tasksToExecute.map((task) => {
      return {
        ...task,

        // replace refs where possible
        args: replaceRefs(
          task.args,
          {
            state: this._state,
          },
          {
            replaceInput: false,
            throwOnFailed: false,
            throwOnNotReady: false,
          },
        ),

        // exclude dependencies that are not in tasksToExecute
        dependencies: task.dependencies.filter((dep) =>
          tasksToExecuteIds.has(dep),
        ),
      };
    });

    return compactedTasks;
  }
}

// replace proxy objects with plain objects
const toPlain = (value: any) => {
  return JSON.parse(JSON.stringify(value));
};

const findRefs = (obj: any): string[] => {
  if (Array.isArray(obj)) {
    return obj.map(findRefs).flat();
  }
  if (obj !== null && typeof obj === 'object') {
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
      if (prop === 'toJSON') {
        return () => obj;
      }
      return createResultReference(taskId, [...path, prop.toString()]);
    },
  }) as any;
};

/**
 * Replaces reference objects in the input with actual values from pipeline state or input.
 * @param obj The object to replace refs in.
 * @param data The data for replacement, including pipeline state and optional input.
 * @param [options] Options for replacement.
 * @param [options.replaceInput=true] Whether to replace $input references.
 * @param [options.throwOnFailed=true] Whether to throw if a referenced task failed.
 * @param [options.throwOnNotReady=true] Whether to throw if a referenced task is not ready.
 * @returns {any} The object with references replaced.
 */
const replaceRefs = (
  obj: any,
  data: { state: PipelineState; input?: any },
  options?: {
    replaceInput?: boolean;
    throwOnFailed?: boolean;
    throwOnNotReady?: boolean;
  },
): any => {
  const {
    replaceInput = true,
    throwOnFailed = true,
    throwOnNotReady = true,
  } = options ?? {};

  if (Array.isArray(obj)) {
    return obj.map((x) => replaceRefs(x, data, options));
  }

  if (obj !== null && typeof obj === 'object') {
    // if obj has $ref property, replace it with the actual value
    if (obj.$ref !== undefined) {
      // Handle special $input reference
      if (obj.$ref === '$input') {
        if (!replaceInput) return obj;

        if (data.input === undefined) {
          return null;
        }
        // return input value at path
        if (obj.path.length === 0) {
          return data.input;
        } else {
          return path(data.input, obj.path);
        }
      }

      const task = data.state[obj.$ref];
      if (task === undefined) {
        if (!throwOnNotReady) return obj;
        throw new TaskNotReadyError(obj.$ref);
      } else if (task.status === TaskStatus.FAILED) {
        if (!throwOnFailed) return obj;
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
        Object.entries(obj).map(([k, v]) => [k, replaceRefs(v, data, options)]),
      );
    }
  }
  return obj;
};
