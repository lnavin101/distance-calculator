import _ from "lodash";
import axios from "axios";
import config from "config";
import { createRequire } from "module";
import util from "util";
import { AsyncParser } from "@json2csv/node";
import { createWriteStream } from "fs";
const require = createRequire(import.meta.url);
const cases = require(`./json/${config.get("filename")}`);
const url = config.get("api.baseUrl");
const apiKey = config.get("api.key");
const outputPath = `${config.get("output.directory")}/${config.get(
  "output.filename"
)}.csv`;
const opts = { header: false };
const transformOpts = {};
const asyncOpts = {};
const parser = new AsyncParser(opts, transformOpts, asyncOpts);
const headerParser = new AsyncParser({}, transformOpts, asyncOpts);
const output = createWriteStream(outputPath, {
  encoding: "utf8",
  flags: "a",
  autoClose: true,
});
var processedRecords = 0;
var errorRecords = 0;

async function processData() {
  let count = 1;
  console.log(
    util.inspect(`Reading data: ${cases?.length} record(s)`, {
      showHidden: false,
      depth: null,
      colors: true,
    })
  );
  for await (const [index, value] of cases.entries()) {
    if (
      !_.isEmpty(value["Latitude, Longitude of Customer Location"]) &&
      !_.isEmpty(value["Location of Driver upon Assignment"])
    ) {
      // to handle rate limit of 10 req/sec
      if (count % 10 == 0) {
        console.log("Sleeping...");
        setTimeout(() => 1500);
      }
      try {
        await getDistance(
          value["Latitude, Longitude of Customer Location"],
          value["Location of Driver upon Assignment"]
        ).then((data) => {
          cases[index]["Approximate Distance"] = data?.distance?.text;
          cases[index]["Approximate Duration"] = data?.duration?.text;
        });
      } catch (e) {
        console.log(e);
        continue;
      }
    }
    count++;
    await appendCsv(cases[index]);
  }
  setTimeout(() => 1000);
  console.log("End Reading");
  //   console.log(
  //     util.inspect(cases, { showHidden: false, depth: null, colors: true })
  //   );
}

async function appendCsv(row) {
  try {
    const csv =
      processedRecords == 0
        ? await headerParser.parse(row).promise()
        : await parser.parse(row).promise();
    let result = output.write(csv+'\n');

    if (result) {
      processedRecords++;
      console.log(`TicketId: ${row["Ticket ID"]} - `, "SUCCESS");
    } else {
      errorRecords++;
      console.log(`TicketId: ${row["Ticket ID"]} - `, "FAILED");
    }
    //console.log("End Writing to File: ", outputPath);
  } catch (e) {
    errorRecords++;
    console.log(`TicketId: ${row["Ticket ID"]} - `, "FAILED");
  }
}

async function getDistance(customerGeo, riderGeo) {
  let customerLocation = _.trim(customerGeo);
  let riderLocation = _.trim(riderGeo);

  let res = await fetchApi(riderLocation, customerLocation);
  //   console.log(
  //     util.inspect(res, { showHidden: false, depth: null, colors: true })
  //   );
  return res?.routes?.car;
}

async function fetchApi(originLoc, destLoc) {
  const params = {
    origin: originLoc,
    destination: destLoc,
    modes: "car",
    units: "metric",
  };

  return await axios
    .get(url, { params: params, headers: { Authorization: apiKey } })
    .then((res) => res.data)
    .catch((err) => console.log(err));
}

console.log('ENV: ',process.env.NODE_ENV);
await processData();

console.log(
  util.inspect(`Done Processing: ${processedRecords} record(s)`, {
    showHidden: false,
    depth: null,
    colors: true,
  })
);
console.log(
  util.inspect(`Error Processing: ${errorRecords} record(s)`, {
    showHidden: false,
    depth: null,
    colors: true,
  })
);
