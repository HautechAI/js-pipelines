const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export const Methods = {
  generateNumber: async () => {
    return 42;
  },

  generateObject: async () => {
    return {
      num: 42,
    };
  },

  multiply: async (a: number, b: number) => {
    return a * b;
  },

  toString: async (a: number | boolean | string) => {
    if (typeof a === "number") {
      return a.toString();
    } else if (typeof a === "boolean") {
      return a ? "true" : "false";
    } else if (typeof a === "string") {
      return a;
    }
    throw new Error("Unsupported type");
  },

  concat: async (a: string, b: string) => {
    return `${a}${b}`;
  },

  sumObjectFields: async (obj: { a: number; b: number }) => {
    return obj.a + obj.b;
  },

  methodWithError: async (input: number) => {
    throw new Error("Error in method");
  },

  methodWithSleep: async () => {
    await sleep(0);
    return "slept";
  },

  // nested methods
  nested: {
    generateString: {
      v1: async () => {
        return "test-string";
      },
    },
  },
};
