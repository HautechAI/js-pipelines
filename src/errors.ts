
export class PipelineError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message);
    this.name = 'PipelineError';
    if (options?.cause) {
      this.cause = options.cause;
    }
  }
}

export class MethodNotFoundError extends PipelineError {
  constructor(method: string, options?: ErrorOptions) {
    super(`Method ${method} not found`);
    this.name = 'MethodNotFoundError';
    if (options?.cause) {
      this.cause = options.cause;
    }
  }
}

export class TaskNotReadyError extends PipelineError {
  constructor(taskId: string, options?: ErrorOptions) {
    super(`Task ${taskId} is not ready`);
    this.name = 'TaskNotReadyError';
    if (options?.cause) {
      this.cause = options.cause;
    }
  }
}
