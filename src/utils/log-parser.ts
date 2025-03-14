import { ParsedLog } from "../types/generic-types";


export function parseLogLine(line: string): ParsedLog | null {

    const regex = /\[(.*?)\]\s+(\w+)\s+(.*?)(?:\s+(\{.*\}))?$/;
    const match = line.match(regex);

    if (!match) {
        return null;
    }

    const [, timestamp, level, message, jsonStr] = match;

    let payload = undefined;
    if (jsonStr) {
        try {
            payload = JSON.parse(jsonStr);
        } catch (error) {
            console.warn(`Failed to parse JSON payload: ${jsonStr}`);
        }
    }

    return {
        timestamp,
        level,
        message,
        payload,
    };
}