type Task = {
  id: string;
  method: string[];
  args: any[];
  dependencies: string[];
};

type UnwrapFunctionResultPromise<T> = T extends (
  ...args: any[]
) => Promise<infer U>
  ? (...args: Parameters<T>) => U
  : never;

type UnwrapFunctionsMap<T> = {
  [Property in keyof T]: T[Property] extends (...args: any[]) => any
    ? UnwrapFunctionResultPromise<T[Property]>
    : UnwrapFunctionsMap<T[Property]>;
};

interface Methods
  extends Record<string, Methods | ((...args: any[]) => Promise<any>)> {}

const path = (obj: any, path: string[]) => {
  const res = path.reduce((acc, key) => acc[key], obj);
  if (res === undefined) {
    throw new Error(`Path ${path.join(".")} not found`);
  }
  return res;
};

const getTaskProxy = (taskId: string, path: string[] = []) => {
  const obj = { $ref: taskId, path };
  return new Proxy(obj, {
    get(target, prop, receiver) {
      if (prop === "toJSON") {
        return () => obj;
      }
      return getTaskProxy(taskId, [...path, prop.toString()]);
    },
  }) as any;
};

export class Pipeline<T extends Methods> {
  constructor(private methods: T) {}

  tasks: Task[] = [];

  state: Record<
    string,
    {
      status: "failed" | "success";
      output: any;
    }
  > = {};

  runningTasks: Set<string> = new Set();

  after(...tasks: any[]) {
    const serializedTasks = JSON.parse(JSON.stringify(tasks));
    return this.createDeferedMethods(
      this.methods,
      this.findRefs(serializedTasks)
    );
  }

  get defer() {
    return this.createDeferedMethods(this.methods);
  }

  async wait<T>(value: T) {
    const serializedValue = JSON.parse(JSON.stringify(value));
    return this.replaceRefs(serializedValue);
  }

  private createDeferedMethods<T extends Methods>(
    methods: T,
    after: string[] = [],
    path: string[] = []
  ) {
    const self = this;

    return new Proxy<UnwrapFunctionsMap<typeof methods>>(methods as any, {
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
          const taskId = `task${self.tasks.length}`;
          self.tasks.push({
            id: taskId,
            method: [...path, prop.toString()],
            args: serializedArgs,
            dependencies: [...self.findRefs(serializedArgs), ...after],
          });
          return getTaskProxy(taskId);
        };
      },
    });
  }

  getTasksReadyToRun() {
    const readyTasks: Task[] = [];
    for (const task of this.tasks) {
      if (
        this.state[task.id]?.status === undefined &&
        task.dependencies.every(
          (dep) => this.state[dep]?.status === "success"
        ) &&
        !this.runningTasks.has(task.id)
      ) {
        readyTasks.push(task);
      }
    }
    return readyTasks;
  }

  async runReadyTasks(continueRunning = false) {
    const tasks = this.getTasksReadyToRun();
    const promises = tasks.map(async (task) => {
      this.startRunningTasks(task.id);
      try {
        const result = await path(
          this.methods,
          task.method
        )(...this.replaceRefs(task.args));
        this.updateteState(task.id, "success", result);
      } catch (e) {
        this.updateteState(task.id, "failed", e);
      } finally {
        this.endRunningTasks(task.id);
      }
      continueRunning && (await this.runReadyTasks(continueRunning));
    });
    await Promise.all(promises);
  }

  startRunningTasks(taskId: string) {
    console.log(`Starting task ${taskId}`);
    this.runningTasks.add(taskId);
  }

  endRunningTasks(taskId: string) {
    console.log(`Ending task ${taskId}`);
    this.runningTasks.delete(taskId);
  }

  updateteState(taskId: string, status: "success" | "failed", output: any) {
    console.log(`Updating task ${taskId} with status ${status}`);
    this.state[taskId] = { status, output };
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
        const task = this.state[obj.$ref];
        if (task === undefined) {
          throw new Error(`Task ${obj.$ref} is not ready`);
        } else if (task.status === "failed") {
          throw task.output;
        } else return path(task.output, obj.path);
      } else {
        // otherwise, replace refs in all the object values
        return Object.fromEntries(
          Object.entries(obj).map(([k, v]) => [k, replaceRefs(v)])
        );
      }
    }
    return obj;
  }
}
