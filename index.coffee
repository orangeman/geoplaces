


if process.argv[2].toLowerCase() == "import"

  if process.argv[3].toLowerCase() == "alts"
    console.log " \n# importing alternate names.."
    require("./geonames").storeAlts()

  else if process.argv[3].toLowerCase() == "geoip"
    console.log "\n importing geo ip ranges.."
    require("geoip-resolve").import("./data/geoip")

  else
    console.log " \n# importing country " + process.argv[3]
    require("./geonames").storeCountry process.argv[3]
    #countries = process.argv[3..-1]
    #(next = () -> gn.storeCountry c, next if c = countries.pop())()


else if process.argv[2].toLowerCase() == "export"

  if process.argv[3].toLowerCase() == "places"
    console.log " \n# exporting places.. "
    require("./geonames").processPlaces()

  else if process.argv[3].toLowerCase() == "names"
    console.log " \n# exporting names.. "
    require("./names")()

  else
    console.log "computing distance table.. "
