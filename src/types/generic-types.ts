export interface ParsedLog {
    timestamp: string;
    level: string;
    message: string;
    payload?: any;
}