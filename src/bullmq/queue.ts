
import { Queue } from 'bullmq';
import redis from '../redis/connection'




export const logProcessingQueue = new Queue('log-processing-queue', {
  connection: redis,
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 1000,
    },
    removeOnComplete: true,
    removeOnFail: false,
  },
});


export async function addLogProcessingJob(fileId: string, filePath: string, fileSize: number) {
  return logProcessingQueue.add(
    'process-log-file',
    { fileId, filePath, fileSize },
    {
      priority: calculatePriority(fileSize),
    }
  );
}


function calculatePriority(fileSize: number): number {
  const MAX_FILE_SIZE = 1024 * 1024 * 100;
  return Math.min(20, Math.max(1, Math.ceil(20 * (fileSize / MAX_FILE_SIZE))));
}