const PROTO_PATH = __dirname + '/protos/route_guide.proto'
const grpc = require('@grpc/grpc-js');
const protoLoader= require('@grpc/proto-loader');
const fs = require('fs');
const parseArgs = require('minimist');
const path = require('path');
const _ = require('lodash');
const packageDefinition = protoLoader.loadSync(
    PROTO_PATH, 
    {keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
       });
const routeguide = grpc.loadPackageDefinition(packageDefinition).routeguide;
const Server = new grpc.Server();

// List of feature objects at points that have been requested so far. 
let feature_list = [];

const checkFeature = (point) => {
    let feature; 
    // Checking if there's a feature object for the given point 
    for(let i =0;i<feature_list.length;i++){
        feature = feature_list[i];
        if(feature.location.latitude === point.latitude && feature.location.longitude === point.longitude){
            return feature;
        }
    }
    let name = '';
    feature = {
        name: name, 
        location: point
    };
    return feature;
};

/**
 * getFeature request handler. Gets a request with a point, and responds with a
 * feature object indicating whether there is a feature at that point.
 * @param {EventEmitter} call Call object for the handler to process
 * @param {function(Error, feature)} callback Response callback
 */
function getFeature(call, callback) {
    callback(null, checkFeature(call.request));
  }


/**
 * Server-Side Streaming RPC - Send back multiple Features to our client
 * listFeatures request handler. Gets a request with two points, and responds
 * with a stream of all features in the bounding box defined by those points.
 * @param {Writable} call Writable stream for responses with an additional
 *     request property for the request value.
 */
function listFeatures(call) {
    const lo = call.request.lo;
    const hi = call.request.hi;
    const left = _.min([lo.longitude, hi.longitude]);
    const right = _.max([lo.longitude, hi.longitude]);
    const top = _.max([lo.latitude, hi.latitude]);
    const bottom = _.min([lo.latitude, hi.latitude]);
    // For each feature, check if it is in the given bounding box
    _.each(feature_list, function(feature) {
      if (feature.name === '') {
        return;
      }
      if (feature.location.longitude >= left &&
          feature.location.longitude <= right &&
          feature.location.latitude >= bottom &&
          feature.location.latitude <= top) {
        call.write(feature);
      }
    });
    call.end();
  }
/**
 * recordRoute handler. Gets a stream of points, and responds with statistics
 * about the "trip": number of points, number of known features visited, total
 * distance traveled, and total time spent.
 * @param {Readable} call The request point stream.
 * @param {function(Error, routeSummary)} callback The callback to pass the
 *     response to
 */
function recordRoute(call, callback) {
    let point_count = 0;
    let feature_count = 0;
    let distance = 0;
    let previous = null;
    // Start a timer
    let start_time = process.hrtime();
    call.on('data', function(point) {
      point_count += 1;
      if (checkFeature(point).name !== '') {
        feature_count += 1;
      }
      /* For each point after the first, add the incremental distance from the
       * previous point to the total distance value */
      if (previous != null) {
        distance += getDistance(previous, point);
      }
      previous = point;
    });
    call.on('end', function() {
      callback(null, {
        point_count: point_count,
        feature_count: feature_count,
        // Cast the distance to an integer
        distance: distance|0,
        // End the timer
        elapsed_time: process.hrtime(start_time)[0]
      });
    });
  }
  
  const route_notes = {};
/**
 * Calculate the distance between two points using the "haversine" formula.
 * The formula is based on http://mathforum.org/library/drmath/view/51879.html.
 * @param start The starting point
 * @param end The end point
 * @return The distance between the points in meters
 */
function getDistance(start, end) {
    function toRadians(num) {
      return num * Math.PI / 180;
    }
    const R = 6371000;  // earth radius in metres
    const lat1 = toRadians(start.latitude / COORD_FACTOR);
    const lat2 = toRadians(end.latitude / COORD_FACTOR);
    const lon1 = toRadians(start.longitude / COORD_FACTOR);
    const lon2 = toRadians(end.longitude / COORD_FACTOR);
  
    const deltalat = lat2-lat1;
    const deltalon = lon2-lon1;
    const a = Math.sin(deltalat/2) * Math.sin(deltalat/2) +
        Math.cos(lat1) * Math.cos(lat2) *
        Math.sin(deltalon/2) * Math.sin(deltalon/2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    return R * c;
  }
/**
 * routeChat handler. Receives a stream of message/location pairs, and responds
 * with a stream of all previous messages at each of those locations.
 * @param {Duplex} call The stream for incoming and outgoing messages
 */
function routeChat(call) {
    call.on('data', function(note) {
      const key = pointKey(note.location);
      /* For each note sent, respond with all previous notes that correspond to
       * the same point */
      if (route_notes.hasOwnProperty(key)) {
        _.each(route_notes[key], function(note) {
          call.write(note);
        });
      } else {
        route_notes[key] = [];
      }
      // Then add the new note to the list
      route_notes[key].push(JSON.parse(JSON.stringify(note)));
    });
    call.on('end', function() {
      call.end();
    });
  }

  function getServer() {
    const server = new grpc.Server();
    server.addService(routeguide.RouteGuide.service, {
      getFeature: getFeature,
      listFeatures: listFeatures,
      recordRoute: recordRoute,
      routeChat: routeChat
    });
    return server;
  }
  if (require.main === module) {
    // If this is run as a script, start a server on an unused port
    const routeServer = getServer();
    routeServer.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), () => {
      const argv = parseArgs(process.argv, {
        string: 'db_path'
      });
      fs.readFile(path.resolve(argv.db_path), function(err, data) {
        if (err) throw err;
        feature_list = JSON.parse(data);
        routeServer.start();
      });
    });
  }
  
  
  exports.getServer = getServer;
  // const routeServer = getServer();
  // routeServer.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), ()=> {
  //     const argv = parseArgs(process.argv, {
  //         string: 'db_path'
  //     })
  //     fs.readFile(path.resolve(argv.db_path), function(err, data) {
  //         if (err) throw err;
  //         feature_list = JSON.parse(data);
  //         routeServer.start();
  //       });
  // })
