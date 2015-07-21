'use strict';

Object.defineProperty(exports, '__esModule', {
    value: true
});

var _slicedToArray = (function () { function sliceIterator(arr, i) { var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i['return']) _i['return'](); } finally { if (_d) throw _e; } } return _arr; } return function (arr, i) { if (Array.isArray(arr)) { return arr; } else if (Symbol.iterator in Object(arr)) { return sliceIterator(arr, i); } else { throw new TypeError('Invalid attempt to destructure non-iterable instance'); } }; })();

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

var _ = require('underscore');
var async = require('async');
var fs = require('fs');
var mkdirp = require('mkdirp');
var readdirp = require('readdirp');
var zlib = require('zlib');
var path = require('path');
var uuid = require('uuid');
var through = require('through2');
var sqlite3 = require('sqlite3').verbose();
var sprintf = require('sprintf-js').sprintf;
var streamify = require('stream-array');

/*
 * new mbutil('/tmp/berlin.mbtiles').exportTo('/tmp/berlin')
 * new mbutil('/tmp/london.mbtiles').importFrom('/tmp/london')
 */

var MButil = (function () {
    function MButil(mbtiles, options) {
        _classCallCheck(this, MButil);

        this.mbtiles = mbtiles;
        this.format = options.format.toLowerCase() || 'png';
        this.scheme = options.scheme.toLowerCase() || 'tms';
        this.compress = options.compress || true;
        this.callback = options.callback || null;
    }

    _createClass(MButil, [{
        key: 'connect',
        value: function connect(cb) {
            this.db = new sqlite3.Database(this.mbtiles);
            this.db.on('error', function (err) {
                console.log(err);
            });
            this.db.on('open', function () {
                console.log(' connected to db');
                cb(null);
            });
        }
    }, {
        key: '_onError',
        value: function _onError(err) {
            if (err) console.log(err);
        }
    }, {
        key: 'setup',
        value: function setup(callback) {
            var db = this.db;
            async.series([function (done) {
                db.run('\n                CREATE TABLE tiles (\n                    zoom_level integer,\n                    tile_column integer,\n                    tile_row integer,\n                    tile_data blob,\n                    CONSTRAINT tile_index UNIQUE (zoom_level, tile_column, tile_row)\n                );', done);
            }, function (done) {
                db.run('\n                CREATE TABLE metadata (\n                    name text,\n                    value text,\n                    CONSTRAINT name UNIQUE (name)\n                );\n            ', done);
            }, function (done) {
                db.run('CREATE TABLE grids (\n                zoom_level integer, tile_column integer, tile_row integer, grid blob);', done);
            }, function (done) {
                db.run('CREATE TABLE grid_data (\n                zoom_level integer, tile_column integer, tile_row integer, key_name text, key_json text);', done);
            }], function (err) {
                console.log(' setup completed');
                callback(err);
            });
        }
    }, {
        key: 'optimizeConnection',
        value: function optimizeConnection(callback) {
            var db = this.db;
            db.serialize(function () {
                db.run('PRAGMA synchronous=0');
                db.run('PRAGMA locking_mode=EXCLUSIVE');
                db.run('PRAGMA journal_mode=DELETE', function () {
                    console.log(' connection optimized');
                    callback();
                });
            });
        }
    }, {
        key: 'optimizeDB',
        value: function optimizeDB(callback) {
            var db = this.db;
            db.serialize(function () {
                db.run('ANALYZE;', function () {
                    console.log(' analyzing db');
                });
                db.run('VACUUM;', function () {
                    console.log(' cleaning db');
                    callback();
                });
            });
        }
    }, {
        key: 'close',
        value: function close(callback) {
            var _this = this;

            async.series([function (done) {
                return _this.optimizeDB(done);
            }, function (done) {
                return _this.db.close(done);
            }], callback);
        }
    }, {
        key: 'compressPrepare',
        value: function compressPrepare(callback) {
            var _this2 = this;

            console.log('Prepare database compression.');

            var db = this.db;
            db.serialize(function () {
                db.run('\n                CREATE TABLE if NOT EXISTS images (\n                    tile_data blob,\n                    tile_id VARCHAR(256));\n            ', _this2._onError);
                db.run('\n                CREATE TABLE if NOT EXISTS map (\n                    zoom_level integer,\n                    tile_column integer,\n                    tile_row integer,\n                    tile_id VARCHAR(256));\n            ', _this2._onError, callback);
            });
        }
    }, {
        key: 'compressDB',
        value: function compressDB(chunk, callback) {
            console.log('Start database compression.');
            var db = this.db;

            db.all('SELECT count(zoom_level) as total FROM tiles', function (err, tiles) {
                if (err) callback(err);

                var total = tiles[0]['total'];
                var range = _.range(Math.floor(total / chunk) + 1);
                console.log('%s total tiles to fetch ', total);

                async.forEach(range, function (i, cb) {
                    console.log(' %s / %s rounds done', i, range[range.length - 1]);
                    var ids = [];
                    var files = [];

                    var _compressTiles = function _compressTiles(err, rows) {
                        if (err) callback(err);

                        async.each(rows, function (r, done) {
                            ++total;
                            if (files.indexOf(r['tile_data']) > 0) {
                                db.run('INSERT INTO map (zoom_level, tile_column, tile_row, tile_id)\n                                values (?, ?, ?, ?)', [r['zoom_level'], r['tile_column'], r['tile_row'], ids[files.indexOf(r['tile_data'])]], done);
                            } else {
                                (function () {
                                    var id = uuid.v4();

                                    ids.push(id);
                                    files.push(r['tile_data']);

                                    db.serialize(function () {
                                        db.run('INSERT INTO images (tile_id, tile_data)\n                                values (?, ?)', [id, r['tile_data']]);
                                        db.run('INSERT INTO map (zoom_level, tile_column, tile_row, tile_id)\n                                values (?, ?, ?, ?)', [r['zoom_level'], r['tile_column'], r['tile_row'], id], done);
                                    });
                                })();
                            }
                        }, cb);
                    };
                    db.all('SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE rowid > ? and rowid <= ?', [i * chunk, (i + 1) * chunk], _compressTiles);
                }, callback);
            });
        }
    }, {
        key: 'compressFinish',
        value: function compressFinish(callback) {
            var db = this.db;
            console.log('Finalizing database compression.');
            db.serialize(function () {
                db.exec('DROP TABLE tiles;');
                db.exec('CREATE VIEW tiles as\n                SELECT map.zoom_level as zoom_level, map.tile_column as tile_column,\n                map.tile_row as tile_row, images.tile_data as tile_data\n                FROM map JOIN images on images.tile_id = map.tile_id;');
                db.exec('CREATE UNIQUE INDEX map_index on map (zoom_level, tile_column, tile_row);');
                db.exec('CREATE UNIQUE INDEX images_id on images (tile_id);');
                db.exec('VACUUM;');
                db.run('ANALYZE;', callback);
            });
        }
    }, {
        key: '_flipY',
        value: function _flipY(zoom, y) {
            return Math.pow(2, zoom) - 1 - y;
        }
    }, {
        key: 'exportMetadata',
        value: function exportMetadata(callback) {
            var _this3 = this;

            async.waterfall([function (cb) {
                var parseMetadata = function parseMetadata(err, metadata) {
                    if (_.isUndefined(metadata) || _.isNull(metadata)) {
                        cb(new Error('metadata is emtpy'));
                    } else {
                        var json_path = path.join(dir, 'metadata.json');
                        fs.writeFile('metadata.json', json_path, JSON.stringify(metadata), function (err) {
                            console.log(' metadata.json dumped');
                            cb(err, metadata);
                        });
                    }
                };

                _this3.db.run('SELECT name, value FROM metadata;', parseMetadata);
            }, function (metadata, cb) {
                var formatter = metadata.formatter;
                var layer_json = path.join(dir, 'layer.json');
                var formatter_json = { formatter: formatter };

                fs.writeFile('layer.json', layer_json, JSON.stringify(formatter_json), function (err) {
                    console.log(' layer.json dumped');
                    cb(err);
                });
            }], function (err) {
                if (err) console.log(err);
                // skip on error, because files are not required
                callback();
            });
        }
    }, {
        key: 'exportTiles',
        value: function exportTiles(dir, callback) {
            var _this4 = this;

            var dumpTiles = function dumpTiles(err, tiles) {
                streamify(tiles).pipe(through.obj(function (tile, enc, cb) {
                    var z = tile.zoom_level;
                    var x = tile.tile_column;
                    var y = tile.tile_row;

                    var tile_dir = null;
                    if (_this4.scheme == 'xyz') {
                        y = _this4._flipY(z, y);
                        tile_dir = path.join(dir, z.toString(), x.toString());
                    } else if (_this4.scheme == 'wms') {
                        tile_dir = path.join(dir, sprintf('%02d', z), sprintf('%03d', parseInt(x) / 1000000), sprintf('%03d', parseInt(x) / 1000 % 1000), sprintf('%03d', parseInt(x) % 1000), sprintf('%03d', parseInt(y) / 1000000), sprintf('%03d', parseInt(y) / 1000 % 1000));
                    } else {
                        tile_dir = path.join(dir, z.toString(), x.toString());
                    }

                    var tile_path = path.join(tile_dir, sprintf('%s.%s', y, _this4.format));
                    if (_this4.scheme == 'wms') {
                        tile_path = path.join(tile_dir, sprintf('%03d.%s', parseInt(y) % 1000, _this4.format));
                    }

                    var putTile = function putTile(err) {
                        if (err) callback(err);
                        fs.writeFile(tile_path, tile['tile_data'], function (err) {
                            console.log(' Write tile %s\t\tto %s', y, tile_path);
                            cb(err);
                        });
                    };
                    mkdirp(tile_dir, putTile);
                })).on('end', callback).pipe(process.stdout);
            };

            this.db.all('SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles;', dumpTiles);
        }
    }, {
        key: 'exportGrids',
        value: function exportGrids(dir, callback) {
            var _this5 = this;

            console.log(' load grid\'s');
            var grid_callback = this.callback;
            var count = 0;
            var total = 0;
            var db = this.db;

            var _exportGrid = function _exportGrid(err, grid) {
                if (err) callback(err);
                if (_.size(grid) == 0) callback(new Error('Grids is empty'));

                var g = _.object(['zoom', 'x', 'y'], grid);
                var zoom = g.zoom;
                var x = g.x;
                var y = g.y;

                if (_this5.scheme == 'xyz') {
                    y = _this5._flipY(zoom, y);
                }

                var grid_dir = path.join(dir, zoom.toString(), x.toString());
                var grid_json = JSON.parse(zlib.unzipSync(grid['grid']));

                var _putGrid = function _putGrid(err) {
                    if (err) console.log(err);

                    db.run('SELECT key_name, key_json FROM grid_data\n                    WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?;\')', [zoom, x, y], function (err, grid_data) {
                        if (err) callback(err);

                        var data = _.map(grid_data, function (key_name) {
                            return key_name.push(grid_data[key_name]);
                        });

                        grid_json['data'] = data;

                        var grid_dump = null;

                        if (grid_callback) {
                            grid_dump = sprintf('%s(%s);', callback, JSON.stringify(grid_json));
                        } else {
                            grid_dump = JSON.stringify(grid_json);
                        }

                        var grid_path = path.join(grid_dir, sprintf('%s.grid.json', y));
                        fs.writeFile(grid_path, grid_dump, function (err) {
                            ++count;
                            console.log(' %s\t/ %s\tgrids exported', count, total);
                            //cb(err)
                        });
                    });
                };

                mkdirp(grid_dir, _putGrid);
            };

            async.series([function (done) {
                db.run('SELECT count(zoom_level) as count FROM grids;', function (err, result) {
                    if (_.isEmpty(result) && _.isUndefined(result)) {
                        done(new Error('Grids is empty'));
                    } else {
                        total = result[0]['count'];
                    }
                });
            }, function (done) {
                db.each('SELECT zoom_level, tile_column, tile_row, grid FROM grids;', _exportGrid, done);
            }], callback);
        }
    }, {
        key: 'exportTo',
        value: function exportTo(dir, callback) {
            var _this6 = this;

            console.log('Export from MBTiles to dir\r\n%s --> %s ', dir, this.mbtiles);
            console.time('mbtiles');

            async.waterfall([function (done) {
                _this6.connect(done);
            }, function (done) {
                // create base dir if not exists
                fs.exists(dir, function (exists) {
                    if (exists) {
                        console.log(new Error('Target directory already exists'));
                        // allow to overwrite
                        done();
                    } else {
                        console.log(' create base directory');
                        fs.mkdir(dir, done);
                    }
                });
            }, function (done) {
                // dump metadata & layer json
                _this6.exportMetadata(done);
            }, function (done) {
                // dump tiles
                _this6.exportTiles(dir, done);
            }, function (done) {
                _this6.exportGrids(dir, done);
            }], function (err) {
                console.timeEnd('mbtiles');
                callback(err);
            });
        }
    }, {
        key: 'insertTile',
        value: function insertTile(tile, content, callback) {
            var zoom = tile.zoom;
            var x = tile.x;
            var y = tile.y;

            console.log(' Read tile from Zoom (z): %s\tCol (x): %s\tRow (y): %s', zoom, x, y);
            this.db.run('INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) values ($zoom, $x, $y, $content);', [zoom, x, y, content], callback);
        }
    }, {
        key: 'insertGrid',
        value: function insertGrid(tile, content, callback) {
            // README: NOT TESTED
            var zoom = tile.zoom;
            var x = tile.x;
            var y = tile.y;

            var db = this.db;

            console.log(' Read grid from Zoom (z): %s\tCol (x): %s\tRow (y): %s', zoom, x, y);
            // Remove potential callback with regex
            var file_content = content.toString();
            var has_callback = file_content.match(/[\w\s=+-/]+\(({(.|\n)*})\);?/);

            if (has_callback) {
                file_content = has_callback[1];
            }

            var utfgrid = JSON.parse(file_content);
            var data = utfgrid.data.pop();

            var compressed = zlib.gzip(JSON.stringify(utfgrid));

            var grid_keys = _.filter(utfgrid['keys'], function (k) {
                if (k != '') {
                    return k;
                }
                return false;
            });

            db.serialize(function () {
                db.run('INSERT INTO grids (zoom_level, tile_column, tile_row, grid) VALUES (?, ?, ?, ?) ', [zoom, x, y, compressed]);

                var gridItem = db.prepare('INSERT INTO grid_data (zoom_level, tile_column, tile_row, key_name, key_json)\n                        VALUES (?, ?, ?, ?, ?);');
                grid_keys.forEach(function (key_name) {
                    var key_json = data[key_name];
                    gridItem.run([zoom, x, y, key_name, JSON.stringify(key_json)]);
                });
                gridItem(callback);
            });
        }
    }, {
        key: 'insertMetadata',
        value: function insertMetadata(meta_file, callback) {
            var _this7 = this;

            fs.exists(meta_file, function (exists) {
                if (!exists) {
                    console.log(new Error('metadata.json not found'));
                    callback();
                }

                fs.readFile(meta_file, 'utf8', function (err, data) {
                    if (err) callback(err);

                    var meta_data = JSON.parse(data);
                    if (_.has(meta_data, 'format')) _this7.format = meta_data.format;
                    var meta = _this7.db.prepare('INSERT INTO metadata (name, value) values (?, ?)');

                    async.forEachOf(meta_data, function (value, key, cb) {
                        meta.run([key, value], cb);
                    }, function (err) {
                        if (err) callback(err);
                        console.log(' metadata from metadata.json restored');
                        meta.finalize(callback);
                    });
                });
            });
        }
    }, {
        key: 'packDir',
        value: function packDir(dir, callback) {
            var _this8 = this;

            var IMAGE_FORMAT = this.format;
            var packItems = through.obj(function (chunk, enc, next) {
                var path = chunk.path;
                var fullPath = chunk.fullPath;

                fs.exists(fullPath, function (exists) {
                    if (!exists) next(new Error('File not exists: ' + fullPath));
                    fs.readFile(fullPath, function (err, content) {
                        if (err) callback(err);
                        //if (path == 'metadata.json') next()

                        var _path$split = path.split('.');

                        var _path$split2 = _slicedToArray(_path$split, 2);

                        var file_name = _path$split2[0];
                        var ext = _path$split2[1];
                        // .pop()
                        var coord = path.replace(/\.[^/.]+$/, '').split('/');

                        if (coord.length != 3) callback(new Error('Fail to parse tile ' + fullPath));

                        var tile = _.object(['zoom', 'x', 'y'], coord);
                        var zoom = tile.zoom;
                        var x = tile.x;
                        var y = tile.y;

                        if (_this8.scheme == 'xyz') {
                            y = _flipY(parseInt(zoom), parseInt(file_name));
                        } else if (_this8.scheme == 'ags') {
                            x = parseInt(file_name.replace('C', ''), 16);
                        } else {
                            y = parseInt(file_name);
                        }

                        if (ext == IMAGE_FORMAT) {
                            _this8.insertTile(tile, content, next);
                        } else if (ext == 'grid.json') {
                            _this8.insertGrid(tile, content, next);
                        }
                    });
                });
            });

            readdirp({ root: dir, depth: 3, fileFilter: [/*'*.json',*/'*.' + IMAGE_FORMAT] }).pipe(packItems).on('end', callback).pipe(process.stdout);
        }
    }, {
        key: 'importFrom',

        // pack tiles from dir
        value: function importFrom(dir, callback) {
            var _this9 = this;

            console.log('Import from directory %s --> %s ', dir, this.mbtiles);
            console.time('mbtiles');

            var meta_file = path.join(dir, 'metadata.json');

            async.series([function (done) {
                _this9.connect(done);
            }, function (done) {
                _this9.optimizeConnection(done);
            }, function (done) {
                _this9.setup(done);
            }, function (done) {
                _this9.insertMetadata(meta_file, done);
            }, function (done) {
                _this9.packDir(dir, done);
            }, function (done) {
                if (_this9.compress == true) {
                    async.series([function (cb) {
                        _this9.compressPrepare(cb);
                    }, function (cb) {
                        var CHUNK = 256;
                        _this9.compressDB(CHUNK, cb);
                    }, function (cb) {
                        _this9.compressFinish(cb);
                    }], done);
                } else {
                    done();
                }
            }, function (done) {
                _this9.close(done);
            }], function (err) {
                console.timeEnd('mbtiles');
                callback(err);
            });
        }
    }]);

    return MButil;
})();

exports['default'] = MButil;
module.exports = exports['default'];