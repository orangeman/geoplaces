fs = require "fs"
i18n = require "http-i18n"

module.exports = (opts, done) ->

  defaults =
    db: "db"
    countries:
      de: "germany"
    languages:
      de: "german"
      en: "english"
    listLength: 7
    charLength: 2

  (opts ||= {})[k] ||= v for k,v of defaults

  if !fs.existsSync opts.db
    fs.mkdirSync opts.db

  gn = require("./geonames")
  countries = (c for c,v of opts.countries)
  load = (i) ->
    if i < countries.length
      gn.storeCountry countries[i], opts.db, () -> load i + 1
    else
      opts.lang = (l for l,v of opts.languages)
      gn.processPlaces opts.lang, opts.db, (places) ->
        require("./names") opts, places, (names) ->
          console.log "places initialized\n"
          done
            autocomplete: autocomplete = (q, lang, cb) ->
              names.get "#{lang}:#{q.toUpperCase()}", (e, n) ->
                cb n || "not found"

            lookup: lookup = (id, cb) ->
              places.get id.toUpperCase(), (e, p) ->
                cb p || error: "not found"

            http: (req, res) ->
              if m = req.url.match /q=(.+)(&|$)/
                lang = i18n req
                autocomplete decodeURI(m[1]), lang, (p) ->
                  res.setHeader "content-type", "text/plain; charset=UTF-8"
                  res.end p
              else if m = req.url.match /place\/(.+)\/?/
                lookup decodeURI(m[1]), (p) ->
                  res.setHeader "content-type", "text/json; charset=UTF-8"
                  res.end JSON.stringify p
              else
                res.end "no place api call"
  load 0
