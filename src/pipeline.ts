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

type Task = {
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
  ? (...args: { [K in keyof P]: WrapRefOrValue<P[K]> } ) => TaskOutput<WrapRef<U>>
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

export class Pipeline<T extends Methods, O> {
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
      tasks?: Task[];
    }
  ) {
    if (options?.state) {
      this._state = options.state;
    }
    if (options?.tasks) {
      options.tasks.forEach((task) => {
        this.addTask(task);
      });
      this.newTaskIndex = this.tasks.length;
    }
  }

  private _output: WrapRefOrValue<O>
  public get output() {
      return this._output;
  }
  public set output(output: WrapRefOrValue<O>) {
    this._output = this.toPlain(output)
  }


  private _tasks: Task[] = [];
  private _tasksById: Record<string, Task> = {};

  private _state: PipelineState = {};

  private runningTasks: Set<string> = new Set();

  private isRunning: boolean = false;

  private newTaskIndex: number = 0;
  private getNewTaskId() {
    let newId: string;
    do {
      newId = `task${this.newTaskIndex++}`;
    } while (this._tasksById[newId] !== undefined);
    return newId;
  }

  private addTask(task: Task) {
    this._tasks.push(task);
    this._tasksById[task.id] = task;
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
            const serializedArgs = self.toPlain(args);
            const taskId = self.getNewTaskId();
            self.addTask({
              id: taskId,
              method: [...path, prop.toString()],
              args: serializedArgs,
              dependencies: [...self.findRefs(serializedArgs), ...after],
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
      result: self.createResultReference(taskId),
      status: () => {
        return self._state[taskId]?.status ?? TaskStatus.PENDING;
      },
      unwrap: async () => {
        return await self.unwrap(self.createResultReference(taskId));
      },
      cancel: () => {
        self._tasks = self._tasks.filter((task) => task.id !== taskId);
        delete self._state[taskId];
      },
      __taskOutput__: true,
    };
  }

  private createResultReference(taskId: string, path: string[] = []) {
    const self = this;

    const obj = { $ref: taskId, path };
    return new Proxy(obj, {
      get(target, prop, receiver) {
        if (prop === "toJSON") {
          return () => obj;
        }
        return self.createResultReference(taskId, [...path, prop.toString()]);
      },
    }) as any;
  }

  private getTasksReadyToRun() {
    const readyTasks: Task[] = [];
    for (const task of this._tasks) {
      if (
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
      this.startRunningTasks(task.id);
      try {
        const method = path(this.methods, task.method);
        if (!method) {
          throw new Error(`Method ${task.method.join(".")} not found`);
        }
        const result = await method(...this.replaceRefs(task.args));
        await this.updateState(task.id, TaskStatus.COMPLETED, result);
      } catch (e) {
        await this.updateState(
          task.id,
          TaskStatus.FAILED,
          this.options?.serializeError ? this.options?.serializeError(e) : e
        );
      } finally {
        this.endRunningTasks(task.id);
      }

      await this.runReadyTasks();
    });
    await Promise.all(promises);
  }

  private startRunningTasks(taskId: string) {
    this.runningTasks.add(taskId);
  }

  private endRunningTasks(taskId: string) {
    this.runningTasks.delete(taskId);
  }

  private async updateState(taskId: string, status: TaskStatus, output: any) {
    this._state[taskId] = { status, output };
    await this.options?.onChangeState?.({ taskId, status, output }, this._state);
  }

  private findRefs(obj: any) {
    const findRefs = this.findRefs.bind(this);

    if (Array.isArray(obj)) {
      return obj.map(findRefs).flat();
    }
    if (typeof obj === "object") {
      if (obj.$ref !== undefined) {
        return obj.$ref;
      } else {
        return Object.values(obj).map(findRefs).flat();
      }
    }
    return [];
  }

  private replaceRefs(obj: any) {
    const replaceRefs = this.replaceRefs.bind(this);

    if (Array.isArray(obj)) {
      return obj.map(replaceRefs);
    }

    if (typeof obj === "object") {
      // if obj has $ref property, replace it with the actual value
      if (obj.$ref !== undefined) {
        const task = this._state[obj.$ref];
        if (task === undefined) {
          throw new Error(`Task ${obj.$ref} is not ready`);
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
          Object.entries(obj).map(([k, v]) => [k, replaceRefs(v)])
        );
      }
    }
    return obj;
  }

  // replace proxy objects with plain objects
  private toPlain(v: any) {
    return JSON.parse(JSON.stringify(v));
  }

  // public methods

  after(...taskIds: string[]) {
    return this.createDeferedMethods(this.methods, taskIds);
  }

  get defer(): ChangeMethodSignaturesInObject<T> {
    return this.createDeferedMethods(this.methods);
  }

  async unwrap<T>(value: T) {
    const serializedValue = this.toPlain(value);
    return this.replaceRefs(serializedValue) as UnwrapRef<T>;
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
    this._output = await this.replaceRefs(this._output);
    const output = this._output as O;

    await this.options?.onCompleted?.(this._state, output);
  }

  get state() {
    return this._state as Readonly<PipelineState>;
  }

  get tasks() {
    return this._tasks as Readonly<Task[]>;
  }

  loadState(state: PipelineState) {
    this._state = state;
  }

  task(taskId: string) {
    if (this._tasks.find((task) => task.id === taskId))
      return this.createTaskObject(taskId);

    throw new Error(`Task ${taskId} not found`);
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
