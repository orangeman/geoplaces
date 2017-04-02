#! /usr/bin/env node

var argv = require('minimist')(process.argv.slice(2));

if (!argv.db) {
  argv.db = "db";
}

if (process.argv[2].toLowerCase() === "import") {

  if (process.argv[3]) {
    require('../geonames').storeCountry(process.argv[3], argv.db);
  }

} else if (process.argv[2].toLowerCase() === "export") {

  if (argv.lang) {
    argv.lang = argv.lang.split(",");
  } else {
    argv.lang = ["en"]
  }

  if (process.argv[3].toLowerCase() === "places") {
    require('../geonames').processPlaces(argv.lang, argv.db);

  } else if (process.argv[3].toLowerCase() === "names") {

    require('../names')(argv);
  }
}
