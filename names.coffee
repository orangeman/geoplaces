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
      console.log JSON.stringify p.value.ambig if p.value.ambig
      collect name: p.value.name, score: p.value.population
      for a,l of p.value.alts
        collect name: a, lang: l, score: p.value.population - 3
      next()
    , () ->
      console.log "\n#{places.length} names with #{collisions} collisions\n"

      a = 0
      c = 0
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
          nms = (p.name[text.length..-1] for p in matches[0..Math.min(matches.length, 21)])
          #console.log lang + text.toUpperCase() + ": " + nms.join ","
          ops.push type: "put", key: lang + text.toUpperCase() + ":", value: nms.join ","

      language = (lang, cb) ->
        a = 0
        c = 0
        ops = []
        done = {}
        console.log "generate for #{lang}.."
        for i in [0..9]
          for p in places
            suggest p.name[0..i].toUpperCase(), lang, p
          console.log " #{i + 1} chars: #{c} lists #{a/c} avg entries"
          a = 0
          c = 0
        names.batch ops, (err) ->
          console.log "+ done #{lang}: " + ops.length + " lists\n"
          cb() if cb

      language "cs", () -> language "de", () -> language "en"
      #language "de"
