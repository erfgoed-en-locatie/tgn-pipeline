var SparqlClient = require('sparql-client'),
    fs = require('fs'),
    util = require('util');

var sparqlFile = 'tgn.sparql',
    sparqlEndpoint = 'http://vocab.getty.edu/sparql.rdf';
    //sparqlEndpoint = 'http://erfgoedenlocatie.cloud.tilaa.com/sparql';

fs.readFile(sparqlFile, 'utf8', function (err, sparqlQuery) {
  // TODO: supply SparqlClient with options to ensure application/json output
  // For now, we have edited SparqlClient's client.js and modified mime-types there.
  var client = new SparqlClient(sparqlEndpoint);

  client.query(sparqlQuery)
    .execute(function(error, results) {
      fs.writeFile('tgn.xml', results, null);
    });
});
