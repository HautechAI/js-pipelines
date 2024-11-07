import { Pipeline } from "./pipeline";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const methods = {
  api: {
    test: async () => {
      await sleep(100);
      return "test";
    },
  },
  generate: async (params: { imageId: string; prompt: string }) => {
    return { id: "operationId1" };
  },
  waitOperation: async (operationId: string) => {
    return {
      images: ["img1", "img2", "img3"],
      preview: "previewImg1",
      operationId: operationId,
    };
  },
  selectImage: async (imageId: string) => {
    return { id: "operationId2", image: imageId };
  },
  createStack: async (operationIds: string[]) => {
    await sleep(110);
    return { id: "stackId1", operations: operationIds };
  },
};

//////

const pipeline = new Pipeline(methods);

const task1 = pipeline.defer.generate({
  imageId: "image1",
  prompt: "test prompt",
});
const task2 = pipeline.defer.waitOperation(task1.id);
const task3 = pipeline.defer.selectImage(task2.images[0]);
const task4 = pipeline.defer.createStack([task1.id, task3.id]);
const task5 = pipeline.defer.api.test();

(async () => {
  console.log(JSON.stringify(pipeline.tasks, null, 2));
  console.log(pipeline.state);
  await pipeline.runReadyTasks(true);
  console.log(pipeline.state);
  console.log(await pipeline.wait(task4.operations));
})();
