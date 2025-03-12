import express from "express"
import cors from "cors"
import Database from "../db/db"
import errorHandler from "../middleware/errorHandler"

class App {
    public app: express.Application
    constructor() {
        this.app = express()
        this.initMiddlewares()
        this.mountRoutes()

    }
    mountRoutes() {
     
        this.app.use(errorHandler as any)

    }
    initMiddlewares() {
        this.app.use(express.json())
        this.app.use(express.urlencoded({ extended: true }))
        this.app.use(cors())
    }
    start(port: number) {
        this.app.listen(port, () => {
            Database.connect()
            console.log(`Server started at http://localhost:${port}`)
        })
    }


}
export default App;