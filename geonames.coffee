exec = require('child_process').exec

csv = require('csv-streamify')
request = require('request')
through = require('through2')
es = require('event-stream')
level = require('level')
fs = require('fs')
zip = require('unzipper')
alt = null
tmp = null

fs.mkdirSync "db" unless fs.existsSync "db"


storeAlts = (db, cb) ->
  console.log "downloading alternative names for all languages\n" + url "alternateNames.zip"
  alt = level "#{db}/alt"
  count = 0
  start = new Date().getTime()
  request.get url "alternateNames.zip"
  .pipe zip.Parse().on "entry", (e) ->
    if e.props.path.match /readme|iso/
      e.autodrain()
    else
      #e.pipe csv delimiter: "\t", objectMode: true
      e.pipe require("split")()
      .on "data", (data) ->
        data = data.toString("utf-8").split "\t"
        #console.log "DATA " + data[1] + ":" + data[2] + ":" + data[4] + ":" + data[5] + ":" + data[6] + " -> " + data[3]
        alt.put data[1] + ":" + data[2] + ":" + data[4] + ":" + data[5] + ":" + data[6], data[3]
        count += 1
        process.stdout.write "   importing #{count}...\r" if count % 100 == 0
      .on "end", () ->
        require('du') "#{db}/alt", (err, size) ->
          process.stdout.write "Done                                        \r"
          console.log "  imported #{count} alternative names\n" +
            "  database size: #{(size/1000000).toFixed 3} MB" +
            "  time #{((new Date().getTime() - start)/60000).toFixed 1} min\n\n"
          cb()


module.exports =

  storeCountry: storeCountry = (country, db, cb) ->
    if typeof db == "function" then cb = db
    else db = "db" unless db
    if !fs.existsSync db + "/alt"
      return storeAlts db, () ->
        storeCountry country, db, cb
    tmp = level "#{db}/tmp", valueEncoding: 'json' unless tmp
    tmp.get "country:#{country}", (err, alreadyDone) ->
      if alreadyDone
        console.log "country #{country} already imported\n"
        cb() if cb
        return
      count = 0
      console.log "importing country #{country}.."
      console.log "  load " + url country.toUpperCase() + ".zip"
      delete require.cache[require.resolve('geonames-stream')]
      request.get url country.toUpperCase() + ".zip"
      .pipe require('geonames-stream').pipeline
      .pipe through.obj (g, enc, next) ->
        if g.feature_code.match(/PPL.*|ADM5/) #&& g.population != "0"
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
          count += 1
          tmp.put "pop:" + normalize(g.population) + ":" + g._id, g, next
          process.stdout.write "   + store #{g.name}" +
            " #{JSON.stringify g.latlon} pop=#{g.population}" +
            "                         \r"
        else if l = g.feature_code.match /ADM(\d)/
          #console.log g.feature_code + ":" + g["admin#{l[1]}_code"]
          tmp.put g.feature_code + ":" + g["admin#{l[1]}_code"], g, next
        else next()
      , () ->
        tmp.put "country:#{country}", country
        tmp.put "changed", changed: true, (err) ->
          process.stdout.write "Done: #{country.toUpperCase()}" +
            "  imported #{count} populated places" +
            "                                               \n\n"
          cb() if cb


  processPlaces: (languages, db, done) ->
    db = "db" unless db
    tmp = level "#{db}/tmp", valueEncoding: 'json' unless tmp
    place = level "#{db}/place", valueEncoding: "json"
    tmp.get "changed", (err, changed) ->
      if not changed
        console.log "places already exported\n"
        done place if done
        return
      start = new Date().getTime()
      place.close () -> exec "rm -r #{db}/place", (err, out) ->
        console.log "exporting places database..   languages #{languages}"
        place = level "#{db}/place", valueEncoding: "json"
        alt = level "#{db}/alt" unless alt
        loadAdminCodes (state) ->
          count = 0
          ambig = 0
          append = 0
          replace = 0
          taken = 0
          alts = 0
          byPopulation()
          #tmp.createReadStream()
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
                      #console.log "   + keep #{p.feature_code} #{p.name} #{p.population}  because batter than  #{v.feature_code} #{v.population}"
                      (p.ambig ||= []).push v
                      place.put k, p, () -> cb p
                      tmp.put "id:" + v._id, k
                    else
                      replace += 1
                      #console.log "   + replace #{p.feature_code} #{p.name} #{p.population}  with  #{v.feature_code} #{v.population}"
                      (v.ambig ||= []).push p
                      place.put k, v, () -> cb v
                      tmp.put "id:" + v._id, k
                  else if i == 0
                    if v.feature_code.match(/PPLX/)
                      #if v.name.match /Bayerbach/
                      #console.log "  SKIP #{v.name} #{v.feature_code} #{v.population}   keep #{p.name} #{p.feature_code} #{p.population}"
                      (p.ambig ||= []).push v
                      place.put k, p, () -> cb p
                      tmp.put "id:" + p._id, k
                      ambig += 1
                      return
                    adm = (ex, ca, l) ->
                      if ca["adm" + l] && ca["adm" + l] != ca.name
                        if v["adm" + l].indexOf(ca.name) >= 0
                          #console.log "   + try ADM#{l} #{ca['adm' + l]} for #{ca.name} #{ca.feature_code} #{ca.population} because already #{ex.feature_code} #{ex.name} #{ex.population}"
                          ca.name = ca["adm" + l]
                        else
                          #if ca.name.match /Bayerbach/
                          #console.log "   + append ADM#{l} #{ex.name} + #{ca['adm' + l]} #{ca.feature_code} #{ca.population} because already #{ex.feature_code} #{ex.name} #{ex.population}"
                          ca.name += (" - " + ca["adm" + l])
                        store ca.name.toUpperCase(), ca, i + 1, cb
                        true
                      else false
                    if !adm(p, v, 4)
                      if !adm(p, v, 3)
                        if !adm(p, v, 2)
                          if !adm(p, v, 1)
                            ambig += 1
                            #console.log "NO PLACE FOR #{k} #{v.feature_code} #{v.population}   already #{p.feature_code} #{p.population}"
                            (p.ambig ||= []).push v
                            place.put k, p, () -> cb p
                            tmp.put "id:" + v._id, k
                  else if i < 2
                    ambig += 1
                    #console.log "NO PLACE FOR #{k} #{v.feature_code} #{v.population}   already #{p.feature_code} #{p.population}"
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
                    console.log "ADM #{g.value.name} -> adm#{l} #{p[ac]} " + adm.name if l == 5 #if p.name == "Bayerbach"
                    delete p[ac]
                    cb p
                  else cb p
              else cb p

            key = g.value.name.toUpperCase()
            g.value.adm1 = state[g.value.country_code][g.value.admin1_code]
            delete g.value.admin1_code
            process.stdout.write "  processing #{key} in #{g.value.adm1}" +
              "                                     \r" if count % 10 == 0

            languages = ["en"] unless languages
            admin g.value, 2, (p2) ->
              admin p2, 3, (p3) ->
                admin p3, 4, (p4) ->
                  store key, p4, 0, (p) ->
                    syn = (i) ->
                      if i < languages.length
                        synonym p, languages[i], () -> syn i + 1
                      else next()
                    syn 0

          , () ->
            tmp.del "changed", (err) ->
              require('du') "#{db}/place", (err, size) ->
                console.log "Done                                           \r"
                console.log "  #{count} names for #{count + ambig} places\n" +
                  "  with #{alts} alternative names (out of #{taken + alts})\n" +
                  "  #{replace} times replaced and #{append} times appended\n" +
                  "  no place for #{ambig} places.\n" +
                  "  places database size #{(size/1000000).toFixed 3} MB  " +
                  "time #{((new Date().getTime() - start) / 60000).toFixed 1} min\n"
                exec "rm -r #{db}/names", (err, out) ->
                  console.log "names db deleted"
                  done(place) if done





url = (file) -> "http://download.geonames.org/export/dump/#{file}"

normalize = (n) ->
  if !n
    n = "0"
  n = "0" + n for i in [0..(8 - n.length)]
  n

byPopulation = (pop) ->
  tmp.createReadStream gte: "pop:" + normalize(pop), lt: "pop:100000000", reverse: true

loadAdminCodes = (cb) ->
  states = {}
  request.get url "admin1CodesASCII.txt"
  .pipe require("split")()
  .on "data", (data) ->
    data = data.toString("utf-8").split "\t"
    code = data[0].split "."
    (states[code[0]] ||= {})[code[1]] = data[2]
  .on "end", () ->
    cb states if cb
