exec = require('child_process').exec
through = require "through2"
level = require "level"


module.exports = () ->

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

  exec "rm -r data/names", (err, out) ->
    console.log "deleted names db"
    names = level "./data/names"
    place = level "./data/place", valueEncoding: "json"

    place.createReadStream()
    .pipe through.obj (p, enc, next) ->
      #console.log JSON.stringify p.value.ambig if p.value.ambig
      console.log "he " + p.value.name if !p.value.country_code
      collect name: p.value.name, id: p.value.name, score: parseInt(p.value.population), adm1: p.value.adm1, country: p.value.country_code
      for a,l of p.value.alts
        collect name: a, lang: l, id: p.value.name, score: (parseInt(p.value.population) + 3), adm1: p.value.adm1, country: p.value.country_code
      next()
    , () ->
      console.log "\n#{places.length} names with #{collisions} collisions\n"

      a = 0
      c = 0
      d = 0
      ops = []
      done = {}
      suggest = (text, lang, pl) ->
        return if pl.lang && pl.lang != lang
        return if done[text]
        done[text] = true
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
                ex[n.id] = n.name
              else
                #console.log "Doublette! " + n.name + "  gibts schon " + ex[n.id]
                #console.log text + ":: " +(p.name[text.length..-1] for p in matches[0..Math.min(matches.length, 7)]).join ","
                matches.splice matches.indexOf(n), 1
                doubletten += 1
                #console.log text + ":: " +(p.name[text.length..-1] for p in matches[0..Math.min(matches.length, 7)]).join ","
          d += doubletten
          nms = matches[0..Math.min(matches.length, 12)].map (p) ->
            "#{p.name[text.length..-1]}, #{p.adm1}, #{p.country}"
          .join "|"
          #nms = (p.name[text.length..-1] + ", " + p.adm1 + ", " + p.country for p in matches[0..Math.min(matches.length, 7)])
          #console.log lang + text.toUpperCase() + ": " + nms
          ops.push type: "put", key: lang + text.toUpperCase() + ":", value: nms

      language = (lang, cb) ->
        a = 0
        c = 0
        d = 0
        ops = []
        done = {}
        console.log "generate for #{lang}.."
        for i in [0..9]
          for p in places
            suggest p.name[0..i].toUpperCase(), lang, p
          console.log " #{i + 1} chars: #{c} lists #{a/c} avg entries (removed #{d/c} Doubletten)"
          a = 0
          c = 0
          d = 0
        names.batch ops, (err) ->
          console.log "+ done #{lang}: #{ops.length} lists\n"
          cb() if cb

      language "cs", () -> language "de", () -> language "en"
      #language "de"

#level("./data/names").createReadStream()
#.pipe through.obj (p, enc, next) ->
#  console.log p.key + " -> " + p.value if p.key.match /FRANKF/
#  next()
