'use strict';

var MButil = require('./lib/index');
var fs = require('fs');

var argv = require('optimist').usage('Usage: $0 --from /path/to/example.mbtiles --to /path/to/dir').demand(['from', 'to']).describe('from', 'Source directory/*.mbtiles to load from').describe('to', 'Destination to *.mbtiles/directory storage')['default']('format', 'png')['default']('scheme', 'tms')['default']('compress', true).describe('format', 'File type format e.g. png').describe('scheme', 'File storage scheme e.g. tms, xyz, ags').describe('compress', 'Do mbtiles compression, Enables by default').argv;

var from = argv.from;
var to = argv.to;

var options = argv;

var callback = function callback(err) {
    if (err) console.log(err);
    console.log('Completed');
};

if (fs.statSync(from).isDirectory()) {
    var mbutil = new MButil(to, options);
    mbutil.importFrom(from, callback);
} else {
    var mbutil = new MButil(from, options);
    mbutil.exportTo(to, callback);
}
