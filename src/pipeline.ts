export enum TaskStatus {
  Success = "success",
  Failed = "failed",
}

export enum PipelineStatus {
  Pending = "pending",
  Running = "running",
  Success = "success",
  Failed = "failed",
}

type Task = {
  id: string;
  method: string[];
  args: any[];
  dependencies: string[];
};

type TaskOutput<T> = {
  id: string;
  result: T;
  cancel: () => void;
};

interface RefPrimitive<T extends string | number | boolean> {
  __kind__: T;
}
type Ref<T> = //
  // if T is a primitive, return a RefPrimitive
  T extends string | number | boolean
    ? RefPrimitive<T>
    : // if T is an array, return an array of Ref<T>
    T extends Array<infer U>
    ? Array<Ref<U>>
    : // if T is an object, return an object with Ref<T> values
    T extends Record<any, any>
    ? { [Property in keyof T]: Ref<T[Property]> }
    : never;

type RefOrValue<T> = //
  // if T is a primitive, return a RefPrimitive
  T extends string | number | boolean
    ? RefPrimitive<T> | T
    : // if T is an array, return an array of Ref<T>
    T extends Array<infer U>
    ? Array<RefOrValue<U>>
    : // if T is an object, return an object with Ref<T> values
    T extends Record<any, any>
    ? { [Property in keyof T]: RefOrValue<T[Property]> }
    : never;

type ChangeMethodSignature<T> = T extends (...args: any[]) => Promise<infer U>
  ? (...args: RefOrValue<Parameters<T>>) => TaskOutput<Ref<U>>
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

interface Methods
  extends Record<string, Methods | ((...args: any[]) => Promise<any>)> {}

const path = (obj: any, path: string[]) => {
  const res = path.reduce((acc, key) => acc[key], obj);
  if (res === undefined) {
    throw new Error(`Path ${path.join(".")} not found`);
  }
  return res;
};

export class Pipeline<T extends Methods> {
  constructor(
    private methods: T,
    private options?: {
      onChangeState?: (
        update: { taskId: string; status: TaskStatus; output: any },
        state: PipelineState
      ) => void;
      state?: PipelineState;
      tasks?: Task[];
    }
  ) {
    if (options?.state) {
      this._state = options?.state;
    }
    if (options?.tasks) {
      options?.tasks.forEach((task) => {
        this.addTask(task);
      });
      this.newTaskIndex = this.tasks.length;
    }
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
            const serializedArgs = JSON.parse(JSON.stringify(args));
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

  private createTaskObject(taskId: string) {
    const self = this;
    return {
      id: taskId,
      result: self.createResultReference(taskId),
      cancel: () => {
        self._tasks = self._tasks.filter((task) => task.id !== taskId);
        delete self._state[taskId];
      },
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
          (dep) => this._state[dep]?.status === TaskStatus.Success
        ) &&
        !this.runningTasks.has(task.id)
      ) {
        readyTasks.push(task);
      }
    }
    return readyTasks;
  }

  private async runReadyTasks(continueRunning = false) {
    const tasks = this.getTasksReadyToRun();
    const promises = tasks.map(async (task) => {
      this.startRunningTasks(task.id);
      try {
        const result = await path(
          this.methods,
          task.method
        )(...this.replaceRefs(task.args));
        this.updateteState(task.id, TaskStatus.Success, result);
      } catch (e) {
        this.updateteState(task.id, TaskStatus.Failed, e);
      } finally {
        this.endRunningTasks(task.id);
      }
      continueRunning && (await this.runReadyTasks(continueRunning));
    });
    await Promise.all(promises);
  }

  private startRunningTasks(taskId: string) {
    // console.log(`Starting task ${taskId}`);
    this.runningTasks.add(taskId);
  }

  private endRunningTasks(taskId: string) {
    // console.log(`Ending task ${taskId}`);
    this.runningTasks.delete(taskId);
  }

  private updateteState(taskId: string, status: TaskStatus, output: any) {
    // console.log(`Updating task ${taskId} with status ${status}`);
    this._state[taskId] = { status, output };
    this.options?.onChangeState?.({ taskId, status, output }, this._state);
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
        } else if (task.status === TaskStatus.Failed) {
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

  // public methods

  after(...taskIds: string[]) {
    return this.createDeferedMethods(this.methods, taskIds);
  }

  get defer() {
    return this.createDeferedMethods(this.methods);
  }

  async wait<T>(value: T) {
    const serializedValue = JSON.parse(JSON.stringify(value));
    return this.replaceRefs(serializedValue);
  }

  get status(): PipelineStatus {
    if (this.isRunning) return PipelineStatus.Running;

    let hasUnprocessedTasks = false;
    for (const task of this._tasks) {
      if (this._state[task.id]?.status === undefined) {
        hasUnprocessedTasks = true;
      }
      if (this._state[task.id]?.status === TaskStatus.Failed) {
        return PipelineStatus.Failed;
      }
    }
    if (hasUnprocessedTasks) return PipelineStatus.Pending;

    return PipelineStatus.Success;
  }

  async run() {
    this.isRunning = true;
    await this.runReadyTasks(true);
    this.isRunning = false;
  }

  get state() {
    return this._state as Readonly<PipelineState>;
  }

  get tasks() {
    return this._tasks as Readonly<Task[]>;
  }

  task(taskId: string) {
    if (this._tasks.find((task) => task.id === taskId))
      return this.createTaskObject(taskId);

    throw new Error(`Task ${taskId} not found`);
  }
}
