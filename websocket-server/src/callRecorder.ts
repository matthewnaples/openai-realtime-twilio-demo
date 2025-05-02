import { createWriteStream, mkdirSync, unlinkSync } from 'fs';
import { spawn } from 'child_process';
import { join } from 'path';

export class CallRecorder {
    private readonly callSid: string;
    private readonly dir: string;
    private readonly rawFilePath: string;
    private readonly outputFilePath: string;
    private writeStream: ReturnType<typeof createWriteStream>;
    private readonly startTime: number;
    private bufferedBytes: number;
    private lastFlushTime: number;
    private readonly FLUSH_INTERVAL_MS = 30000; // 30 seconds
    private readonly FLUSH_SIZE_BYTES = 500 * 1024; // 500KB
    private isFinalized: boolean = false;

    constructor(callSid: string) {
        this.callSid = callSid;
        this.startTime = Date.now();
        this.lastFlushTime = this.startTime;
        this.bufferedBytes = 0;

        // Create recordings directory structure
        this.dir = join(process.cwd(), 'recordings', callSid);
        mkdirSync(this.dir, { recursive: true });

        const timestamp = new Date(this.startTime).toISOString().replace(/[:.]/g, '-');
        this.rawFilePath = join(this.dir, `raw-${timestamp}.mulaw`);
        this.outputFilePath = join(this.dir, `recording-${timestamp}.wav`);

        // Create write stream with error handling
        this.writeStream = createWriteStream(this.rawFilePath);
        this.writeStream.on('error', (err) => {
            console.error(`Error writing to recording file for call ${this.callSid}:`, err);
        });
    }

    write(base64Audio: string): void {
        if (this.isFinalized) {
            console.warn('Attempted to write to finalized recorder');
            return;
        }

        try {
            const buffer = Buffer.from(base64Audio, 'base64');
            this.writeStream.write(buffer);
            this.bufferedBytes += buffer.length;

            // Check if we need to flush based on time or size
            const now = Date.now();
            if (now - this.lastFlushTime >= this.FLUSH_INTERVAL_MS ||
                this.bufferedBytes >= this.FLUSH_SIZE_BYTES) {
                this.flush();
            }
        } catch (err) {
            console.error(`Error processing audio chunk for call ${this.callSid}:`, err);
        }
    }

    private flush(): void {
        // Instead of using flush(), we'll just track the buffer size and time
        this.lastFlushTime = Date.now();
        this.bufferedBytes = 0;
    }

    async finalize(): Promise<string> {
        if (this.isFinalized) {
            return this.outputFilePath;
        }

        this.isFinalized = true;
        this.writeStream.end();

        return new Promise((resolve, reject) => {
            const ffmpeg = spawn('ffmpeg', [
                '-f', 'mulaw',
                '-ar', '8000',
                '-ac', '1',
                '-i', this.rawFilePath,
                '-y', // Overwrite output file if it exists
                this.outputFilePath
            ]);

            ffmpeg.stderr.on('data', (data) => {
                console.log(`ffmpeg stderr for call ${this.callSid}: ${data}`);
            });

            ffmpeg.on('close', (code) => {
                if (code === 0) {
                    try {
                        unlinkSync(this.rawFilePath); // Clean up raw file
                        resolve(this.outputFilePath);
                    } catch (err) {
                        console.error(`Error cleaning up raw file for call ${this.callSid}:`, err);
                        resolve(this.outputFilePath); // Still resolve with output path even if cleanup fails
                    }
                } else {
                    reject(new Error(`ffmpeg exited with code ${code} for call ${this.callSid}`));
                }
            });

            ffmpeg.on('error', (err) => {
                reject(new Error(`ffmpeg error for call ${this.callSid}: ${err.message}`));
            });
        });
    }
} 