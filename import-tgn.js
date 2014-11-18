var fs = require('fs'),
    util = require('util'),
    xml2js = require('xml2js'),
    pg = require('pg'),
    elasticsearch = require('elasticsearch');

// Expects BAG to be available in database BAG, with username and password 'postgres'
var conString = "postgres://postgres:postgres@localhost/bag",
    bagQuery = "SELECT identificatie::int AS id, ST_AsGeoJSON(ST_ForceRHR(ST_Force2D(ST_Transform(geovlak, 4326)))) AS geom FROM woonplaatsactueelbestaand WHERE woonplaatsnaam = $1";

var pgClient = new pg.Client(conString);
pgClient.connect(function(err) {
  if (err) {
    return console.error('could not connect to postgres', err);
  }
  parseXML(closeConnection);
});

var esClient = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'trace'
});

function closeConnection() {
  pgClient.end();
}

function parseXML(done) {
  var parser = new xml2js.Parser();
  fs.readFile(__dirname + '/tgn.xml', function(err, data) {
    parser.parseString(data, function (err, result) {
      var count = result['rdf:RDF']['rdf:Description'].length;
      result['rdf:RDF']['rdf:Description'].forEach(function(element, index) {
        createDocument(element, function() {
          if (index == count - 1) {
            done();
          }
        });
      });
    });
  });
}

function getElementTagValue(element, tag) {
  if (element[tag] && element[tag].length > 0 && element[tag][0]['_']) {
    return element[tag][0]['_'];
  }
  return null;
}

function getElementTagAttribute(element, tag, attribute) {
  if (element[tag] && element[tag].length > 0 && element[tag][0]['$'] && element[tag][0]['$'][attribute]) {
    return element[tag][0]['$'][attribute];
  }
  return null;
}

function createDocument(element, callback) {
  var label = getElementTagValue(element, 'rdfs:label'),
      source = getElementTagAttribute(element, 'dc-term:source', 'rdf:resource'),
      term = getElementTagValue(element, 'gvp:term'),
      long = getElementTagValue(element, 'geo-pos:long'),
      lat = getElementTagValue(element, 'geo-pos:lat'),
      startDate = getElementTagValue(element, 'schema:startDate'),
      endDate = getElementTagValue(element, 'schema:endDate');

  var geometry = getBAGGeometryByName(label, function(id, geometry) {
    if (geometry) {
      console.log("Found " + label + "in BAG, adding to Elasticsearch...");

      var doc = {
        "uri": "http://data.erfgeo.nl/grs/Place/Amstelodamum/1",
        //"date_created": "2014-11-14 12:00:55",
        "source": {
          "name": term,
          "uri": source,
          //"startDate": 1764,
          //"endDate": 1894,
          "dataset": "tgn"
          // "geometry": {
          //   "type": "Point",
          //   "coordinates": [4.9040, 52.3702]
          // }
        },
        "relationship": {
          //"created": "2014-11-14 14:04:41",
          "author": "bert@waag.org",
          "type": "grs:approximation"
          //"uri": "http://data.erfgeo.nl/grs/Relationship/Amstelodamum/1"
        },
        "target": {
          "name": label,
          "uri": "http://lod.geodan.nl/basisreg/bag/woonplaats/id_" + id,
          "startDate": 2014,
          "endDate": null,
          "dataset": "bag",
          "geometry": geometry
        }
      };

      esClient.create({
        index: 'pelias',
        type: 'pit',
        body: doc
      }, function (error, response) {
        console.log(response);
        callback();
      });
    } else {
      // Not found in BAG, don't do anything!
      callback();
    }
  });
}

function getBAGGeometryByName(name, callback) {
  // TODO: use pg-query? (https://github.com/brianc/node-pg-query)
  pgClient.query(bagQuery.replace("$1", "'" + name.replace("'", "''") + "'"), function(err, result) {
    if (err) {
      callback(null);
    }
    if (result.rows.length > 0) {
      callback(result.rows[0].id, JSON.parse(result.rows[0].geom));
    } else {
      callback(null);
    }
  });
}
