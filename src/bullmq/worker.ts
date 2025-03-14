
import { Job, Worker } from 'bullmq';
import fs from 'fs';
import readline from 'readline';
import { supabase } from '../db/db';
import { parseLogLine } from '../utils/log-parser';
import redis from '../redis/connection'


const keywordsToTrack = process.env.KEYWORDS_TO_TRACK
  ? process.env.KEYWORDS_TO_TRACK.split(',')
  : ['error', 'fail', 'exception'];


export function startWorker() {
  const worker = new Worker(
    'log-processing-queue',
    async (job) => {
      const { fileId, filePath } = job.data;
      console.log(`Processing job ${job.id} for file: ${filePath}`);

      try {

        const stats = {
          fileId,
          total_lines: 0,
          errors: 0,
          warnings: 0,
          keywords: {} as Record<string, number>,
          ips: {} as Record<string, number>,
          started_at: new Date(),
          completed_at: null as Date | null,
          status: 'processing' as 'processing' | 'completed' | 'failed',
        };


        const fileStream = fs.createReadStream(filePath);
        const rl = readline.createInterface({
          input: fileStream,
          crlfDelay: Infinity,
        });


        for await (const line of rl) {
          stats.total_lines++;


          const parsedLog = parseLogLine(line);

          if (parsedLog) {

            if (parsedLog.level.toUpperCase() === 'ERROR') {
              stats.errors++;
            } else if (parsedLog.level.toUpperCase() === 'WARNING' || parsedLog.level.toUpperCase() === 'WARN') {
              stats.warnings++;
            }


            for (const keyword of keywordsToTrack) {
              if (parsedLog.message.toLowerCase().includes(keyword.toLowerCase())) {
                stats.keywords[keyword] = (stats.keywords[keyword] || 0) + 1;
              }
            }


            const ipMatches = [...parsedLog.message.matchAll(/\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b/g)];
            if (ipMatches.length > 0) {
              for (const match of ipMatches) {
                const ip = match[0];
                stats.ips[ip] = (stats.ips[ip] || 0) + 1;
              }
            }


            if (parsedLog.payload && parsedLog.payload.ip) {
              const ip = parsedLog.payload.ip;
              stats.ips[ip] = (stats.ips[ip] || 0) + 1;
            }
          }


          if (stats.total_lines % 1000 === 0) {
            await job.updateProgress(stats);
          }
        }


        stats.completed_at = new Date();
        stats.status = 'completed';


        const { error } = await supabase
          .from('log_stats')
          .upsert({
            file_id: stats.fileId,
            total_lines: stats.total_lines,
            errors: stats.errors,
            warnings: stats.warnings,
            keywords: stats.keywords,
            ips: stats.ips,
            started_at: stats.started_at.toISOString(),
            completed_at: stats.completed_at.toISOString(),
            status: stats.status,
          });

        if (error) {
          console.error('Error storing stats in Supabase:', error);
          throw new Error(`Failed to store stats: ${error.message}`);
        }

        console.log(`Successfully processed job ${job.id} for file: ${filePath}`);
        return stats;
      } catch (error) {
        console.error(`Error processing job ${job.id}:`, error);


        await supabase
          .from('log_stats')
          .upsert({
            file_id: fileId,
            status: 'failed',
            completed_at: new Date().toISOString(),
          });

        throw error;
      }
    },
    {
      connection: redis,
      concurrency: 4,
      autorun: true,
    }
  );


  worker.on('completed', (job) => {
    console.log(`Job ${job.id} completed successfully`);
  });

  worker.on('failed', (job: Job | undefined, error) => {
    console.error(`Job ${job?.id} failed with error: ${error.message}`);
  });

  worker.on('error', (err) => {
    console.error('Worker error:', err);
  });

  console.log('Log processing worker started');
  return worker;
}

// Initialize the worker if this file is executed directly
if (require.main === module) {
  startWorker();
}