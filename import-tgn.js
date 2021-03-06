var fs = require('fs'),
    util = require('util'),
    xml2js = require('xml2js'),
    pg = require('pg'),
    elasticsearch = require('elasticsearch');

var esHostname = 'erfgoedenlocatie.cloud.tilaa.com:9200';
//var esHostname = 'localhost:9200';

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
  host: esHostname
});

function closeConnection() {
  pgClient.end();
}

function parseXML(done) {
  var parser = new xml2js.Parser();
  fs.readFile(__dirname + '/tgn.xml', function(err, data) {
    parser.parseString(data, function (err, result) {
      var count = result['rdf:RDF']['rdf:Description'].length;
      6
      result['rdf:RDF']['rdf:Description'].forEach(function(element, index) {
        createDocument(element, function() {
          count--;
          if (count <= 0) {
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
  } else if (element[tag] && element[tag].length > 0) {
    return element[tag][0];
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
      console.log("Found " + term + " = " + label + " in BAG, adding to Elasticsearch...");
      var now = new Date().toISOString();
      var doc = {
        "uri": "http://data.erfgeo.nl/grs/Place/" + term + "/1",
        "date_created": now,
        "source": {
          "name": term,
          "uri": source,
          "dataset": "tgn"
        },
        "relationship": {
          "created": now,
          "author": "bert@waag.org",
          "type": "grs:approximation",
          "uri": "http://data.erfgeo.nl/grs/Relationship/" + term + "/1"
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

      if (startDate) {
        doc.source.startDate = startDate;
      }

      if (endDate) {
        doc.source.endDate = endDate;
      }

      esClient.create({
        index: 'pelias',
        type: 'pit',
        body: doc
      }, function (error, response) {
        callback();
      });
    } else {
      // Not found in BAG, don't do anything!
      callback();
    }
  });
}

// TODO: find way to round coordinates without corrupting polygons
//function round_coordinates(str) {
//  return str.replace(/(\d+)\.(\d{6})\d+/g, '$1.$2');
//}

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
