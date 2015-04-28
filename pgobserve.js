var fs = require('fs');
var path = require('path');
var pg = require('pg');
var pgFormat = require('pg-format');
var async = require('async');
var shortid = require('shortid');
var cliTable = require('cli-table');
var _ = require('lodash');

// avoid dash
shortid.characters(
  '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_$');

var observeFunction = fs.readFileSync(
  path.join(__dirname, 'pgobserve.py'), 'utf8');
var conString = 'postgres://localhost/postgres';

if (process.argv.length < 4) {
  console.error('usage: node pgobserve.js QUERY table1 table2 table3...');
  process.exit(1);
}
var query = process.argv[2];
var tableNames = process.argv.slice(3);

var client = new pg.Client(conString);
var backendPid;
var observeId = shortid.generate();
var listenChannel;
var triggersToDrop = [];

client.on('notification', function (msg) {
  console.log('Notified; polling!');
  needToPollQuery();
});

// This queue only allows one thing to run at once.
var pollQueue = async.queue(pollQuery, 1);
function needToPollQuery () {
  // If there's already a non-started poll on the queue, ignore.
  if (pollQueue.length()) {
    return;
  }
  pollQueue.push({});
};

var previousOutput = null;
function pollQuery (task, cb) {
  client.query(query, function (err, result) {
    if (err) {
      console.error("Query error", err);
      return;
    }
    var output;
    if (! result.rows.length) {
      output = "Empty result!";
    } else {
      var columnNames = _.keys(result.rows[0]);
      var table = new cliTable({
        head: columnNames
      });
      result.rows.forEach(function (row) {
        table.push(_.map(columnNames, function (columnName) {
          return row[columnName];
        }));
      });
      output = table.toString();
    }
    if (previousOutput === output) {
      console.log("We were asked to repoll, but nothing changed!");
    } else {
      console.log(output);
      previousOutput = output;
    }
    cb();
  });
};

function cleanupTriggers (exitCode) {
  async.eachSeries(triggersToDrop, function (t, cb) {
    client.query(
      pgFormat('DROP TRIGGER IF EXISTS %I ON %I', t.trigger, t.table),
      cb);
  }, function (err) {
    if (err) {
      console.error("Cleanup error", err);
      process.exit(1);
    }
    process.exit(exitCode);
  });
};

async.waterfall([
  // Connect.
  function (cb) {
    client.connect(cb);
  },
  // Ensure existence of triggered function.
  function (nullResult, cb) {
    client.query(observeFunction, cb);
  },
  // Figure out the current session's process ID.
  function (result, cb) {
    client.query('SELECT pg_backend_pid();', cb);
  },
  function (pidResult, cb) {
    // Listen for changes.
    backendPid = pidResult.rows[0].pg_backend_pid;
    listenChannel = 'notify_' + backendPid + '_' + observeId;
    client.query(pgFormat('LISTEN %I', listenChannel), cb);
  },
  function (result, cb) {
    // Set up triggers.
    async.eachSeries(tableNames, function (tableName, cb) {
      var triggerName = 'observe_' + backendPid + '_' + observeId;
      triggersToDrop.push({ trigger: triggerName, table: tableName });
      async.series([
        function (cb) {
          client.query(
            pgFormat('DROP TRIGGER IF EXISTS %I ON %I', triggerName, tableName),
            cb);
        },
        function (cb) {
          client.query(
            pgFormat(
              ('CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %I ' +
               'EXECUTE PROCEDURE observe(%L, %L, %L);'),
              triggerName, tableName, query, observeId, listenChannel),
            cb);
        }
      ], cb);
    }, cb);
  },
  function (cb) {
    // Register to drop triggers. (If we fail, a background superuser process
    // can use pg_stat_activity to find current pids and clean up those that
    // aren't current.)
    process.once('beforeExit', function () {
      cleanupTriggers(0);
    });
    process.on('SIGINT', function () {
      cleanupTriggers(1);
    });
    process.on('SIGTERM', function () {
      cleanupTriggers(1);
    });
    cb();
  },
  function (cb) {
    console.log("Observing!");
    needToPollQuery();
  }
], function (error) {
  if (error) {
    console.error("Got error:", error);
    process.exit(1);
  }
});
