'use strict';

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
var sprintf = require("sprintf-js").sprintf;
var streamify = require('stream-array');

/*
 * new mbutil('/tmp/berlin.mbtiles').exportTo('/tmp/berlin')
 * new mbutil('/tmp/london.mbtiles').importFrom('/tmp/london')
 */
export default class MButil {

    constructor(mbtiles, options) {
        this.mbtiles = mbtiles;
        this.format = options.format.toLowerCase() || 'png';
        this.scheme = options.scheme.toLowerCase() || 'tms';
        this.compress = options.compress || true;
        this.callback = options.callback || null;
    }

    connect(cb) {
        this.db = new sqlite3.Database(this.mbtiles);
        this.db.on('error', (err)=> {
            console.log(err)
        });
        this.db.on('open', ()=> {
            console.log(' connected to db');
            cb(null)
        })
    }

    _onError(err) {
        if (err) console.log(err)
    }

    setup(callback) {
        let db = this.db;
        async.series([
            (done)=> {
                db.run(`
                CREATE TABLE tiles (
                    zoom_level integer,
                    tile_column integer,
                    tile_row integer,
                    tile_data blob,
                    CONSTRAINT tile_index UNIQUE (zoom_level, tile_column, tile_row)
                );`, done)
            },
            (done)=> {
                db.run(`
                CREATE TABLE metadata (
                    name text,
                    value text,
                    CONSTRAINT name UNIQUE (name)
                );
            `, done)
            },
            (done)=> {
                db.run(`CREATE TABLE grids (
                zoom_level integer, tile_column integer, tile_row integer, grid blob);`,
                    done)
            },
            (done)=> {
                db.run(`CREATE TABLE grid_data (
                zoom_level integer, tile_column integer, tile_row integer, key_name text, key_json text);`,
                    done)
            }
        ], (err)=> {
            console.log(' setup completed');
            callback(err)
        });
    }

    optimizeConnection(callback) {
        let db = this.db;
        db.serialize(()=> {
            db.run(`PRAGMA synchronous=0`);
            db.run(`PRAGMA locking_mode=EXCLUSIVE`);
            db.run(`PRAGMA journal_mode=DELETE`, ()=> {
                console.log(' connection optimized');
                callback()
            });

        })
    }

    optimizeDB(callback) {
        let db = this.db;
        db.serialize(()=> {
            db.run(`ANALYZE;`, ()=> {
                console.log(' analyzing db')
            });
            db.run(`VACUUM;`, ()=> {
                console.log(' cleaning db');
                callback()
            });
        });
    }

    close(callback) {
        async.series([
            (done)=> this.optimizeDB(done),
            (done)=> this.db.close(done)
        ], callback)
    }

    compressPrepare(callback) {
        console.log('Prepare database compression.');

        let db = this.db;
        db.serialize(()=> {
            db.run(`
                CREATE TABLE if NOT EXISTS images (
                    tile_data blob,
                    tile_id VARCHAR(256));
            `, this._onError);
            db.run(`
                CREATE TABLE if NOT EXISTS map (
                    zoom_level integer,
                    tile_column integer,
                    tile_row integer,
                    tile_id VARCHAR(256));
            `, this._onError, callback);
        });
    }

    compressDB(chunk, callback) {
        console.log('Start database compression.');
        let db = this.db;

        db.all(`SELECT count(zoom_level) as total FROM tiles`, (err, tiles)=> {
            if (err) callback(err);

            var total = tiles[0]['total'];
            let range = _.range(Math.floor(total / chunk) + 1);
            console.log("%s total tiles to fetch ", total);

            async.forEach(range, (i, cb)=> {
                console.log(" %s / %s rounds done", i, range[range.length - 1]);
                var ids = [];
                var files = [];

                var _compressTiles = (err, rows)=> {
                    if (err) callback(err);

                    async.each(rows, (r, done)=> {
                        ++total;
                        if (files.indexOf(r['tile_data']) > 0) {
                            db.run(`INSERT INTO map (zoom_level, tile_column, tile_row, tile_id)
                                values (?, ?, ?, ?)`,
                                [r['zoom_level'], r['tile_column'], r['tile_row'], ids[files.indexOf(r['tile_data'])]], done);
                        } else {
                            let id = uuid.v4();

                            ids.push(id);
                            files.push(r['tile_data']);

                            db.serialize(()=> {
                                db.run(`INSERT INTO images (tile_id, tile_data)
                                values (?, ?)`, [id, r['tile_data']]);
                                db.run(`INSERT INTO map (zoom_level, tile_column, tile_row, tile_id)
                                values (?, ?, ?, ?)`, [r['zoom_level'], r['tile_column'], r['tile_row'], id], done);
                            })
                        }
                    }, cb);
                };
                db.all(`SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE rowid > ? and rowid <= ?`,
                    [(i * chunk), ((i + 1) * chunk)], _compressTiles);

            }, callback);
        });
    }

    compressFinish(callback) {
        let db = this.db;
        console.log('Finalizing database compression.');
        db.serialize(()=> {
            db.exec(`DROP TABLE tiles;`);
            db.exec(`CREATE VIEW tiles as
                SELECT map.zoom_level as zoom_level, map.tile_column as tile_column,
                map.tile_row as tile_row, images.tile_data as tile_data
                FROM map JOIN images on images.tile_id = map.tile_id;`);
            db.exec(`CREATE UNIQUE INDEX map_index on map (zoom_level, tile_column, tile_row);`);
            db.exec(`CREATE UNIQUE INDEX images_id on images (tile_id);`);
            db.exec(`VACUUM;`);
            db.run(`ANALYZE;`, callback);
        })
    }

    _flipY(zoom, y) {
        return Math.pow(2, zoom) - 1 - y
    }

    exportMetadata(callback) {
        async.waterfall([
            (cb)=> {
                let parseMetadata = (err, metadata)=> {
                    if (_.isUndefined(metadata) || _.isNull(metadata)) {
                        cb(new Error('metadata is emtpy'))
                    } else {
                        let json_path = path.join(dir, 'metadata.json');
                        fs.writeFile('metadata.json', json_path, JSON.stringify(metadata), (err)=> {
                            console.log(' metadata.json dumped');
                            cb(err, metadata)
                        });
                    }
                };

                this.db.run(`SELECT name, value FROM metadata;`, parseMetadata)

            },
            (metadata, cb)=> {
                let formatter = metadata.formatter;
                let layer_json = path.join(dir, 'layer.json');
                let formatter_json = {formatter};

                fs.writeFile('layer.json', layer_json, JSON.stringify(formatter_json), (err)=> {
                    console.log(' layer.json dumped');
                    cb(err)
                })
            }
        ], (err)=> {
            if (err) console.log(err);
            // skip on error, because files are not required
            callback()
        })
    }

    exportTiles(dir, callback) {
        let dumpTiles = (err, tiles)=> {
            streamify(tiles)
                .pipe(
                through.obj((tile, enc, cb)=> {
                    let z = tile.zoom_level;
                    let x = tile.tile_column;
                    let y = tile.tile_row;

                    let tile_dir = null;
                    if (this.scheme == 'xyz') {
                        y = this._flipY(z, y);
                        tile_dir = path.join(dir, z.toString(), x.toString())
                    } else if (this.scheme == 'wms') {
                        tile_dir = path.join(dir,
                            sprintf('%02d', (z)),
                            sprintf('%03d', (parseInt(x) / 1000000)),
                            sprintf('%03d', ((parseInt(x) / 1000) % 1000)),
                            sprintf('%03d', (parseInt(x) % 1000)),
                            sprintf('%03d', (parseInt(y) / 1000000)),
                            sprintf('%03d', ((parseInt(y) / 1000) % 1000)))
                    } else {
                        tile_dir = path.join(dir, z.toString(), x.toString())
                    }

                    let tile_path = path.join(tile_dir, sprintf('%s.%s', y, this.format));
                    if (this.scheme == 'wms') {
                        tile_path = path.join(tile_dir, sprintf('%03d.%s', (parseInt(y) % 1000), this.format))
                    }

                    let putTile = (err)=> {
                        if (err) callback(err);
                        fs.writeFile(tile_path, tile['tile_data'], (err)=> {
                            console.log(' Write tile %s\t\tto %s', y, tile_path);
                            cb(err)
                        })
                    };
                    mkdirp(tile_dir, putTile);
                })
            ).on('end', callback)
                .pipe(process.stdout);
        };

        this.db.all(`SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles;`, dumpTiles)
    }

    exportGrids(dir, callback) {
        console.log(' load grid\'s');
        let grid_callback = this.callback;
        let count = 0;
        let total = 0;
        let db = this.db;

        let _exportGrid = (err, grid)=> {
            if (err) callback(err);
            if (_.size(grid) == 0) callback(new Error('Grids is empty'));

            let g = _.object(['zoom', 'x', 'y'], grid);
            let {zoom, x, y} = g;

            if (this.scheme == 'xyz') {
                y = this._flipY(zoom, y)
            }

            let grid_dir = path.join(dir, zoom.toString(), x.toString());
            let grid_json = JSON.parse(zlib.unzipSync(grid['grid']));

            let _putGrid = (err)=> {
                if (err) console.log(err);

                db.run(`SELECT key_name, key_json FROM grid_data
                    WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?;')`,
                    [zoom, x, y], (err, grid_data)=> {
                        if (err) callback(err);

                        let data = _.map(grid_data, (key_name)=> {
                            return key_name.push(grid_data[key_name])
                        });

                        grid_json['data'] = data;

                        let grid_dump = null;

                        if (grid_callback) {
                            grid_dump = sprintf('%s(%s);', callback, JSON.stringify(grid_json))
                        } else {
                            grid_dump = JSON.stringify(grid_json);
                        }

                        let grid_path = path.join(grid_dir, sprintf('%s.grid.json', y));
                        fs.writeFile(grid_path, grid_dump, (err)=> {
                            ++count;
                            console.log(' %s\t/ %s\tgrids exported', count, total);
                            //cb(err)
                        })
                    });
            };

            mkdirp(grid_dir, _putGrid);
        };

        async.series([
            (done) => {
                db.run('SELECT count(zoom_level) as count FROM grids;', (err, result)=> {
                    if (_.isEmpty(result) && _.isUndefined(result)) {
                        done(new Error('Grids is empty'));
                    } else {
                        total = result[0]['count']
                    }
                });
            },
            (done)=> {
                db.each('SELECT zoom_level, tile_column, tile_row, grid FROM grids;',
                    _exportGrid, done);
            }
        ], callback);


    }

    exportTo(dir, callback) {
        console.log("Export from MBTiles to dir\r\n%s --> %s ", dir, this.mbtiles);
        console.time('mbtiles');

        async.waterfall([
            (done)=> {
                this.connect(done)
            },
            (done)=> {
                // create base dir if not exists
                fs.exists(dir, (exists)=> {
                    if (exists) {
                        console.log(new Error('Target directory already exists'));
                        // allow to overwrite
                        done();
                    } else {
                        console.log(' create base directory');
                        fs.mkdir(dir, done)
                    }
                });
            },
            (done)=> {
                // dump metadata & layer json
                this.exportMetadata(done)
            },
            (done)=> {
                // dump tiles
                this.exportTiles(dir, done)
            },
            (done)=> {
                this.exportGrids(dir, done)

            }
        ], (err)=> {
            console.timeEnd('mbtiles');
            callback(err);
        })
    }

    insertTile(tile, content, callback) {
        let {zoom, x, y} = tile;
        console.log(" Read tile from Zoom (z): %s\tCol (x): %s\tRow (y): %s", zoom, x, y);
        this.db.run(`INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) values ($zoom, $x, $y, $content);`,
            [zoom, x, y, content], callback);
    }

    insertGrid(tile, content, callback) {
        // README: NOT TESTED
        let {zoom, x, y} = tile;
        let db = this.db;

        console.log(' Read grid from Zoom (z): %s\tCol (x): %s\tRow (y): %s', zoom, x, y);
        // Remove potential callback with regex
        let file_content = content.toString();
        let has_callback = file_content.match(/[\w\s=+-/]+\(({(.|\n)*})\);?/);

        if (has_callback) {
            file_content = has_callback[1];
        }

        let utfgrid = JSON.parse(file_content);
        let data = utfgrid.data.pop();

        let compressed = zlib.gzip(JSON.stringify(utfgrid));

        let grid_keys = _.filter(utfgrid['keys'], (k)=> {
            if (k != '') {
                return k
            }
            return false
        });

        db.serialize(()=> {
            db.run(`INSERT INTO grids (zoom_level, tile_column, tile_row, grid) VALUES (?, ?, ?, ?) `,
                [zoom, x, y, compressed]);

            let gridItem = db.prepare(`INSERT INTO grid_data (zoom_level, tile_column, tile_row, key_name, key_json)
                        VALUES (?, ?, ?, ?, ?);`);
            grid_keys.forEach((key_name)=> {
                let key_json = data[key_name];
                gridItem.run([zoom, x, y, key_name, JSON.stringify(key_json)])
            });
            gridItem(callback)
        })
    }

    insertMetadata(meta_file, callback) {

        fs.exists(meta_file, (exists)=> {
            if (!exists) {
                console.log(new Error('metadata.json not found'));
                callback();
            }

            fs.readFile(meta_file, 'utf8', (err, data)=> {
                if (err) callback(err);

                let meta_data = JSON.parse(data);
                if (_.has(meta_data, 'format')) this.format = meta_data.format;
                let meta = this.db.prepare(`INSERT INTO metadata (name, value) values (?, ?)`);

                async.forEachOf(meta_data, (value, key, cb)=> {
                    meta.run([key, value], cb);
                }, (err)=> {
                    if (err) callback(err);
                    console.log(' metadata from metadata.json restored');
                    meta.finalize(callback);
                });
            });
        });
    }

    packDir(dir, callback) {
        const IMAGE_FORMAT = this.format;
        let packItems = through.obj((chunk, enc, next)=> {
            let {path, fullPath} = chunk;

            fs.exists(fullPath, (exists)=> {
                if (!exists) next(new Error('File not exists: ' + fullPath));
                fs.readFile(fullPath, (err, content)=> {
                    if (err) callback(err);
                    //if (path == 'metadata.json') next()

                    let [file_name, ext] = path.split('.'); // .pop()
                    let coord = path.replace(/\.[^/.]+$/, '').split('/');

                    if (coord.length != 3) callback(new Error('Fail to parse tile ' + fullPath));

                    let tile = _.object(['zoom', 'x', 'y'], coord);
                    let {zoom, x, y} = tile;

                    if (this.scheme == 'xyz') {
                        y = _flipY(parseInt(zoom), parseInt(file_name))
                    }
                    else if (this.scheme == 'ags') {
                        x = parseInt(file_name.replace('C', ''), 16)
                    }
                    else {
                        y = parseInt(file_name)
                    }

                    if (ext == IMAGE_FORMAT) {
                        this.insertTile(tile, content, next);

                    } else if (ext == 'grid.json') {
                        this.insertGrid(tile, content, next);
                    }

                })
            })
        });

        readdirp({root: dir, depth: 3, fileFilter: [/*'*.json',*/ '*.' + IMAGE_FORMAT]})
            .pipe(packItems).on('end', callback)
            .pipe(process.stdout)
    }

    // pack tiles from dir
    importFrom(dir, callback) {
        console.log("Import from directory %s --> %s ", dir, this.mbtiles);
        console.time('mbtiles');

        let meta_file = path.join(dir, 'metadata.json');

        async.series([
            (done)=> {
                this.connect(done);
            },
            (done)=> {
                this.optimizeConnection(done)
            },
            (done)=> {
                this.setup(done)
            },
            (done)=> {
                this.insertMetadata(meta_file, done)
            },
            (done)=> {
                this.packDir(dir, done)
            },
            (done)=> {
                if (this.compress == true) {
                    async.series([
                        (cb)=> {
                            this.compressPrepare(cb)
                        },
                        (cb)=> {
                            const CHUNK = 256;
                            this.compressDB(CHUNK, cb)
                        },
                        (cb)=> {
                            this.compressFinish(cb)
                        }
                    ], done)
                } else {
                    done()
                }
            },
            (done)=> {
                this.close(done)
            }

        ], (err)=> {
            console.timeEnd('mbtiles');
            callback(err)
        });
    }
}
