
gn = require "./geonames"





if process.argv[2].toLowerCase() == "import"

  if process.argv[3].toLowerCase() == "alts"
    console.log " \n# importing alternate names.."
    gn.storeAlts()

  else
    console.log " \n# importing country " + process.argv[3]
    gn.storeCountry process.argv[3]
    #countries = process.argv[3..-1]
    #(next = () -> gn.storeCountry c, next if c = countries.pop())()


else if process.argv[2].toLowerCase() == "export"

  if process.argv[3].toLowerCase() == "places"
    console.log " \n# exporting places.. "
    gn.processPlaces()

  else if process.argv[3].toLowerCase() == "names"
    console.log " \n# exporting names.. "
    require("./names")()

  else
    console.log "computing distance table.. "
