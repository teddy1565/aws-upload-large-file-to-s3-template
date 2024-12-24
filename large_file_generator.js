const fs = require("fs");

let str = "A";
for (let i = 0; i < 10; i++) {
    fs.appendFileSync("./test.txt", str.repeat(1024 * 1024 * 100));
}