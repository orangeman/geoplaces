exec = require('child_process').exec

csv = require('csv-streamify')
request = require('request')
through = require('through2')
es = require('event-stream')
level = require('level')
fs = require('fs')
alt = level "./data/alt"
tmp = level "./data/tmp", valueEncoding: 'json'

#tmp.createReadStream gte: "ADM", lt: "ADM~"
#.pipe es.mapSync (p) -> console.log "#{p.key} -> #{p.value.name}"


module.exports =

  storeAlts: () ->
    fs.createReadStream "./alternateNames.txt"
    .pipe csv delimiter: "\t", objectMode: true
    .on "data", (data) ->
      #console.log "DATA " + data[1] + ":" + data[2] + ":" + data[4] + ":" + data[5] + ":" + data[6] + " -> " + data[3]
      alts.put data[1] + ":" + data[2] + ":" + data[4] + ":" + data[5] + ":" + data[6], data[3]


  storeCountry: (country) ->
    console.log "   load " + url country
    request.get url country
    .pipe require('geonames-stream').pipeline
    .pipe through.obj (g, enc, next) ->
      if g.feature_code.match(/PPL.*|ADM5/) && g.population != "0"
        g.latlon = [parseFloat(g.latitude), parseFloat(g.longitude)]
        delete g.latitude
        delete g.longitude
        g.name = g.name
          .replace("Bezirk", "")
          .replace(",", " ")
          .replace("'", "")
          .replace("ˈ", "")
          .trim()
        delete g.alternatenames
        tmp.put "pop:" + normalize(g.population) + ":" + g._id, g, next
        console.log "   + store #{g.name} #{JSON.stringify g.latlon} pop=#{g.population}"
      else if l = g.feature_code.match /ADM(\d)/
        #console.log g.feature_code + ":" + g["admin#{l[1]}_code"]
        tmp.put g.feature_code + ":" + g["admin#{l[1]}_code"], g, next
      else next()
    , () -> console.log "done"


  processPlaces: () ->
    exec "rm -r data/place", (err, out) ->
      console.log " deleted place db"
      place = level "./data/place", valueEncoding: "json"
      count = 0
      ambig = 0
      append = 0
      replace = 0
      taken = 0
      alts = 0
      byPopulation 1 * 1000
      .pipe through.obj (g, enc, next) ->

        store = (k, v, i, cb) ->
          place.get k, (err, p) ->
            if !p
              count += 1 if i < 2
              append += 1 if i == 1
              alts += 1 if i == 2
              place.put k, v, () -> cb v
              if i < 2
                tmp.put "id:" + v._id, k
              #console.log " + place #{count}: #{k} #{JSON.stringify v.latlon} pop=#{v.population} #{i}"
            else
              if p.feature_code.match(/ADM/) && v.feature_code.match(/PPL/)
                if !v.population
                  ambig += 1
                  console.log "   + keep #{p.feature_code} #{p.name} #{p.population}  because batter than  #{v.feature_code} #{v.population}"
                  (p.ambig ||= []).push v
                  place.put k, p, () -> cb p
                  tmp.put "id:" + v._id, k
                else
                  replace += 1
                  console.log "   + replace #{p.feature_code} #{p.name} #{p.population}  with  #{v.feature_code} #{v.population}"
                  (v.ambig ||= []).push p
                  place.put k, v, () -> cb v
                  tmp.put "id:" + v._id, k
              else if i == 0
                if v.feature_code.match(/PPLX/)
                  if v.name.match /Bayerbach/
                    console.log "SKIP #{v.name} #{v.feature_code} #{v.population}   keep #{p.name} #{p.feature_code} #{p.population}"
                  (p.ambig ||= []).push v
                  place.put k, p, () -> cb p
                  tmp.put "id:" + p._id, k
                  ambig += 1
                  return
                adm = (ex, ca, l) ->
                  if ca["adm" + l] && ca["adm" + l] != ca.name
                    if v["adm" + l].indexOf(ca.name) >= 0
                      #console.log "   #{k} try ADM#{l} #{ca['adm' + l]} for #{ca.name} #{ca.feature_code} #{ca.population} because already #{ex.feature_code} #{ex.name} #{ex.population}"
                      ca.name = ca["adm" + l]
                    else
                      if ca.name.match /Bayerbach/
                        console.log "   #{k} append ADM#{l} #{ex.name} + #{ca['adm' + l]} #{ca.feature_code} #{ca.population} because already #{ex.feature_code} #{ex.name} #{ex.population}"
                      ca.name += (" - " + ca["adm" + l])
                    store ca.name.toUpperCase(), ca, i + 1, cb
                    true
                  else false
                if !adm(p, v, 4)
                  if !adm(p, v, 3)
                    if !adm(p, v, 2)
                      if !adm(p, v, 1)
                        ambig += 1
                        console.log "   NO PLACE FOR #{k} #{v.feature_code} #{v.population}   already #{p.feature_code} #{p.population}"
                        (p.ambig ||= []).push v
                        place.put k, p, () -> cb p
                        tmp.put "id:" + v._id, k
              else if i < 2
                ambig += 1
                console.log "   NO PLACE FOR #{k} #{v.feature_code} #{v.population}   already #{p.feature_code} #{p.population}"
                (p.ambig ||= []).push v
                place.put k, p, () -> cb p
                tmp.put "id:" + v._id, k
              else
                taken += 1
                cb()

        synonym = (p, c, cb) ->
          alt.createReadStream gte: g.value._id, lte: g.value._id + "~"
          .pipe through.obj (a, enc, nextAlt) ->
            k = a.key.split(":")[1]
            if k == c
              a.value = a.value
                .replace("Bezirk", "")
                .replace(",", " ")
                .replace("'", "")
                .replace("ˈ", "")
                .trim()
              store a.value.toUpperCase(), p, 2, (at) ->
                if at
                  if !(p.alts ||= {})[a.value]
                    p.alts[a.value] = c
                    place.put kk.toUpperCase(), p for kk,u of p.alts
                    place.put p.name.toUpperCase(), p, nextAlt
                  else
                    console.log " - no #{a.value} #{a.key}   gibt schon #{p.alts[c]}"
                    nextAlt()
                else nextAlt()
            else nextAlt()
          , () -> cb()

        admin = (p, l, cb) ->
          ac = "admin#{l}_code"
          if p[ac]
            tmp.get "ADM#{l}:#{p[ac]}", (err, adm) ->
              if adm
                p["adm" + l] = adm.name
                #console.log "#{g.value.name} -> adm#{l} #{p[ac]} " + adm.name if l == 5 #if p.name == "Bayerbach"
                delete p[ac]
                cb p
              else cb p
          else cb p

        key = g.value.name.toUpperCase()
        g.value.adm1 = state[g.value.country_code][g.value.admin1_code]
        delete g.value.admin1_code

        admin g.value, 2, (p2) ->
          admin p2, 3, (p3) ->
            admin p3, 4, (p4) ->
              store key, p4, 0, (p) ->
                synonym p, "cs", () ->
                  synonym p, "de", () ->
                    synonym p, "en", next

      , () -> console.log " done #{count} keys for #{count + ambig} places with #{alts} synonyms of #{taken + alts}," +
        " Therof #{replace} times replaced and #{append} times appended. No place for #{ambig} places."




  computeDists: () ->
    exec "rm -rf data/dist data/path", (err, out) ->
      console.log "deleted dist and path db"
      dist = level "./data/dist"
      path = level "./data/path"
      time = new Date().getTime()
      count = 0
      byPopulation 20
      .pipe through.obj (from, enc, nextFrom) ->
        byPopulation 20
        .pipe through.obj (to, enc, nextTo) =>
          @push [from, to]
          nextTo()
        .on "end", nextFrom
      .pipe through.obj (route, enc, next) ->
        console.log "+ compute " + route[0].name + " -> " + route[1].name




url = (country) -> "http://download.geonames.org/" +
  "export/dump/#{country.toUpperCase() || "DE"}.zip"

normalize = (n) ->
  if !n
    n = "0"
  else
    n = "0" + n
  n = "0" + n for i in [0..(8 - n.length)]
  n

byPopulation = (pop) ->
  tmp.createReadStream gte: "pop:" + normalize(pop), lt: "pop:10000000", reverse: true


state =
  "AT":
    "09":	"Wien"
    "08":	"Vorarlberg"
    "07":	"Tirol"
    "06":	"Steiermark"
    "05":	"Salzburg"
    "04":	"Oberösterreich"
    "03":	"Niederösterreich"
    "02":	"Kärnten"
    "01":	"Burgenland"
  "CZ":
    "52":	"Praha"
    "78":	"South Moravian"
    "79":	"Jihočeský"
    "80":	"Vysočina"
    "81":	"Karlovarský"
    "82":	"Královéhradecký"
    "83":	"Liberecký"
    "84":	"Olomoucký"
    "85":	"Moravskoslezský"
    "86":	"Pardubický"
    "87":	"Plzeňský"
    "88":	"Central Bohemia"
    "89":	"Ústecký"
    "90":	"Zlín"
  "DE":
    "15": "Thüringen"
    "10":	"Schleswig-Holstein"
    "14":	"Sachsen-Anhalt"
    "13":	"Sachsen"
    "09":	"Saarland"
    "08":	"Rheinland-Pfalz"
    "07":	"Nordrhein-Westfalen"
    "06":	"Niedersachsen"
    "12":	"Mecklenburg-Vorpommern"
    "05":	"Hessen"
    "04":	"Hamburg"
    "03":	"Bremen"
    "11":	"Brandenburg"
    "16":	"Berlin"
    "02":	"Bayern"
    "01":	"Baden-Württemberg"

