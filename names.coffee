exec = require('child_process').exec
through = require "through2"
level = require "level"
fs = require "fs"

module.exports = (opts, place, done) ->
  places = []
  existing = {}
  collisions = 0

  collect = (p) ->
    unless existing[p.lang + p.name]
      existing[p.lang + p.name] = p
      places.push p
    else
      collisions += 1
      #console.log "name collision! #{JSON.stringify(existing[p.name])}\n  no place for #{JSON.stringify(p)}"

  opts.listLength ||= 5
  opts.charLength ||= 3
  names = level "#{opts.db}/names"
  place = level "#{opts.db}/place", valueEncoding: "json" unless place
  names.get "latest", (err, alreadyDone) ->
    if alreadyDone
      alreadyDone = JSON.parse alreadyDone
      if alreadyDone.listLength == opts.listLength && alreadyDone.charLength == opts.charLength
        toDo = []
        (toDo.push l if alreadyDone.lang.indexOf(l) < 0) for l in opts.lang
        if toDo.length == 0
          console.log "no names to export\n"
          done names if done
          return

    names.close () -> exec "rm -r #{opts.db}/names", (err, out) ->
      names = level "#{opts.db}/names"
      place.createReadStream()
      .pipe through.obj (p, enc, next) ->
        #console.log JSON.stringify p.value.ambig if p.value.ambig
        console.log "he " + p.value.name if !p.value.country_code
        collect name: p.value.name, id: p.value.name, score: parseInt(p.value.population), adm1: p.value.adm1, country: p.value.country_code, alts: p.value.alts
        for a,l of p.value.alts
          collect name: a, lang: l, id: p.value.name, score: (parseInt(p.value.population) + 3), adm1: p.value.adm1, country: p.value.country_code
        next()
      , () ->
        console.log "exporting #{places.length} names with #{collisions} collisions\n"

        a = 0
        c = 0
        d = 0
        ops = []
        checked = {}
        suggest = (text, lang, pl) ->
          return if pl.lang && pl.lang != lang
          return if checked[text]
          checked[text] = true
          matches = places
            .filter( (p) ->
              (!p.lang || p.lang == lang) &&
              p.name.toUpperCase().indexOf(text) == 0)
            .sort( (n, m) -> m.score - n.score)
          if matches.length > 0
            c += 1
            a += matches.length
            doubletten = 0
            ex = {}
            for n in matches
              if n
                if !ex[n.id]
                  ex[n.id] = n
                  if lang == "ar" && !n.lang
                    ar = (alt for alt, ll of n.alts when ll == "ar")
                    if ar.length > 0
                      n.ar = n.name + " " + ar[0]
                else
                  #console.log "Doublette! " + n.name + "  exists " + ex[n.id].name
                  #console.log text + ":: " +(p.name[text.length..-1] for p in matches[0..Math.min(matches.length, 7)]).join ","
                  matches.splice matches.indexOf(n), 1
                  doubletten += 1
                  #console.log text + ":: " +(p.name[text.length..-1] for p in matches[0..Math.min(matches.length, 7)]).join ","
            d += doubletten
            nms = matches[0..Math.min(matches.length, opts.listLength)].map (p) ->
              if lang == "ar"
                "#{(p.ar || p.name)[text.length..-1]}, #{p.adm1}, #{p.country}"
              else
                "#{p.name[text.length..-1]}, #{p.adm1}, #{p.country}"
            .join "|"
            #nms = (p.name[text.length..-1] + ", " + p.adm1 + ", " + p.country for p in matches[0..Math.min(matches.length, 7)])
            #console.log lang + text.toUpperCase() + ": " + nms
            ops.push type: "put", key: lang + ":" + text.toUpperCase(), value: nms

        language = (l) ->
          a = 0
          c = 0
          d = 0
          ops = []
          checked = {}
          lang = opts.lang[l]
          console.log "generate autocompletion lists for language #{lang}.."
          for i in [0..opts.charLength - 1]
            for p in places
              suggest p.name[0..i].toUpperCase(), lang, p
            console.log " #{i + 1} chars: #{c} lists " +
              "#{(a/c).toFixed 1} avg entries " +
              "(removed #{(d/c).toFixed 1} Doubletten)"
            a = 0
            c = 0
            d = 0
          names.batch ops, (err) ->
            console.log " done #{lang}: #{ops.length} lists\n"
            if l < opts.lang.length - 1
              language l + 1
            else
              names.put "latest", JSON.stringify(opts), (err) ->
                console.log "ready" unless err
                done names if done
        language 0
#
# level("db/names").createReadStream()
# .on "data", (p) ->
#   console.log p.key + " -> " + p.value# if p.key.match /FRANKF/
