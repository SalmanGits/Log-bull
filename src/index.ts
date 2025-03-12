import App from "./app/app";
import dotenv from "dotenv";
dotenv.config();

const app = new App()
const PORT: number = Number(process.env.PORT) || 3000

app.start(PORT)